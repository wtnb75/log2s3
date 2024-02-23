import unittest
from unittest.mock import patch
import tempfile
import json
from click.testing import CliRunner
from log2s3.main import cli


class TestIble(unittest.TestCase):
    toml = """
[params]
dotenv = true
compress = "xz"
top = "/var/log/container"

[filetree-compress]
name = "compress old files"
older = "1d"
newer = "7d"
bigger = "10k"

[s3-make-bucket]
name = "make bucket"
allow-fail = true

[s3-put-tree]
name = "upload older files"
older = "2d"
newer = "7d"

[filetree-delete]
name = "delete oldest files"
older = "400d"
"""

    jsonlist = [{
        "name": "params",
        "params": {
            "dotenv": True,
            "compress": "xz",
            "top": "/var/log/container",
        },
    }, {
        "name": "compress old files",
        "filetree-compress": {
            "older": "1d",
            "newer": "7d",
            "bigger": "10k",
        },
    }, {
        "name": "make bucket",
        "allow-fail": True,
        "s3-make-bucket": {},
    }, {
        "name": "upload older files",
        "s3-put-tree": {
            "older": "2d",
            "newer": "7d",
        },
    }, {
        "name": "delete oldest files",
        "filetree-delete": {
            "older": "400d",
        },
    }]
    shstr = """#! /bin/sh
set -eu

# compress old files
log2s3 filetree-compress --older 1d --newer 7d --bigger 10k --compress xz --top /var/log/container

# make bucket
log2s3 s3-make-bucket --dotenv || true

# upload older files
log2s3 s3-put-tree --dotenv --older 2d --newer 7d --compress xz --top /var/log/container

# delete oldest files
log2s3 filetree-delete --older 400d --compress xz --top /var/log/container

"""

    def test_convert_toml2json(self):
        with tempfile.NamedTemporaryFile("r+") as tf:
            tf.write(self.toml)
            tf.flush()
            res = CliRunner().invoke(cli, ["ible-convert", tf.name, "--format", "json"])
            if res.exception:
                raise res.exception
            data = json.loads(res.output)
            self.assertEqual(self.jsonlist, data)

    def test_convert_toml2sh(self):
        with tempfile.NamedTemporaryFile("r+") as tf:
            tf.write(self.toml)
            tf.flush()
            res = CliRunner().invoke(cli, ["ible-convert", tf.name, "--format", "sh"])
            if res.exception:
                raise res.exception
            self.assertEqual(self.shstr, res.output)

    def test_playbook_dry(self):
        with tempfile.NamedTemporaryFile("r+") as tf:
            tf.write(self.toml)
            tf.flush()
            with self.assertLogs(level="INFO") as alog:
                res = CliRunner().invoke(cli, ["ible-playbook", tf.name, "--dry"])
            if res.exception:
                raise res.exception
            self.assertIn("(dry)", "\n".join(alog.output))

    def test_playbook_wet(self):
        with tempfile.NamedTemporaryFile("r+") as tf:
            tf.write(self.toml)
            tf.flush()
            with self.assertLogs(level="INFO") as alog, \
                    patch("log2s3.main.filetree_compress.callback") as fc, \
                    patch("log2s3.main.s3_put_tree.callback") as spt, \
                    patch("log2s3.main.filetree_delete.callback") as fd:
                res = CliRunner().invoke(cli, ["ible-playbook", tf.name])
            if res.exception:
                raise res.exception
            self.assertNotIn("(dry)", "\n".join(alog.output))
            fc.assert_called_once()
            self.assertEqual(
                {"older": "1d", "newer": "7d", "bigger": "10k", "top": "/var/log/container",
                 "compress": "xz"},
                {k: v for k, v in fc.call_args.kwargs.items() if bool(v)})
            spt.assert_called_once()
            self.assertEqual(
                {"older": "2d", "newer": "7d", "top": "/var/log/container",
                 "compress": "xz", "dotenv": True},
                {k: v for k, v in spt.call_args.kwargs.items() if bool(v)})
            fd.assert_called_once()
            self.assertEqual({
                "older": "400d", "top": "/var/log/container",
                "compress": "xz",  # need no compress option...
            }, {k: v for k, v in fd.call_args.kwargs.items() if bool(v)})
