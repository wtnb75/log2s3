import unittest
from unittest.mock import patch
import tempfile
import json
from click.testing import CliRunner
from log2s3.main import cli
from click.core import UNSET


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
s3_secret_key = "mysecretkey"

[filetree-delete]
name = "delete oldest files"
older = "400d"
"""

    jsonlist = [
        {
            "name": "params",
            "params": {
                "dotenv": True,
                "compress": "xz",
                "top": "/var/log/container",
            },
        },
        {
            "name": "compress old files",
            "filetree-compress": {
                "older": "1d",
                "newer": "7d",
                "bigger": "10k",
            },
        },
        {
            "name": "make bucket",
            "allow-fail": True,
            "s3-make-bucket": {},
        },
        {
            "name": "upload older files",
            "s3-put-tree": {
                "older": "2d",
                "newer": "7d",
                "s3_secret_key": "mysecretkey",
            },
        },
        {
            "name": "delete oldest files",
            "filetree-delete": {
                "older": "400d",
            },
        },
    ]
    jsonlist_ex = [
        {
            "name": "compress old files",
            "filetree-compress": {
                "older": "1d",
                "newer": "7d",
                "bigger": "10k",
                "compress": "xz",
                "top": "/var/log/container",
            },
        },
        {
            "name": "make bucket",
            "allow-fail": True,
            "s3-make-bucket": {"dotenv": True},
        },
        {
            "name": "upload older files",
            "s3-put-tree": {
                "older": "2d",
                "newer": "7d",
                "compress": "xz",
                "top": "/var/log/container",
                "s3_secret_key": "mysecretkey",
                "dotenv": True,
            },
        },
        {
            "name": "delete oldest files",
            "filetree-delete": {
                "older": "400d",
                "top": "/var/log/container",
            },
        },
    ]
    shstr = """#! /bin/sh
set -eu

# compress old files
log2s3 filetree-compress --bigger 10k --compress xz --newer 7d --older 1d --top /var/log/container

# make bucket
log2s3 s3-make-bucket --dotenv || true

# upload older files
log2s3 s3-put-tree --compress xz --dotenv --newer 7d --older 2d --s3-secret-key mysecretkey --top /var/log/container

# delete oldest files
log2s3 filetree-delete --older 400d --top /var/log/container

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
        with tempfile.NamedTemporaryFile("r+") as tf, patch("os.getenv") as og:
            og.return_value = None
            tf.write(self.toml)
            tf.flush()
            res = CliRunner().invoke(cli, ["ible-convert", tf.name, "--format", "sh"])
            if res.exception:
                raise res.exception
            og.assert_any_call("AWS_ACCESS_KEY_ID")
            og.assert_any_call("AWS_ENDPOINT_URL_S3")
            self.assertEqual(self.shstr, res.output)

    def test_convert_sh2json(self):
        with tempfile.NamedTemporaryFile("r+") as tf:
            tf.write(self.shstr)
            tf.flush()
            res = CliRunner().invoke(cli, ["ible-convert", tf.name, "--format", "json"])
            if res.exception:
                raise res.exception
            self.assertEqual(self.jsonlist_ex, json.loads(res.output))

    def test_playbook_dry(self):
        with tempfile.NamedTemporaryFile("r+") as tf:
            tf.write(self.toml)
            tf.flush()
            with self.assertLogs(level="INFO") as alog:
                res = CliRunner().invoke(cli, ["ible-playbook", tf.name, "--dry"])
            if res.exception:
                raise res.exception
            self.assertIn("(dry)", "\n".join(alog.output))
            self.assertNotIn("mysecretkey", "\n".join(alog.output))

    def test_playbook_wet(self):
        with tempfile.NamedTemporaryFile("r+") as tf:
            tf.write(self.toml)
            tf.flush()
            with (
                self.assertLogs(level="INFO") as alog,
                patch("log2s3.main.filetree_compress.callback") as fc,
                patch("log2s3.main.s3_put_tree.callback") as spt,
                patch("log2s3.main.filetree_delete.callback") as fd,
                patch("os.getenv") as og,
            ):
                og.return_value = None
                res = CliRunner().invoke(cli, ["ible-playbook", tf.name, "--verbose"])
            if res.exception:
                raise res.exception
            print("\n".join(alog.output))
            self.assertNotIn("(dry)", "\n".join(alog.output))
            self.assertNotIn("mysecretkey", "\n".join(alog.output))
            og.assert_any_call("AWS_ACCESS_KEY_ID")
            og.assert_any_call("AWS_ENDPOINT_URL_S3")
            fc.assert_called_once()
            self.assertEqual(
                {
                    "older": "1d",
                    "newer": "7d",
                    "bigger": "10k",
                    "top": "/var/log/container",
                    "compress": "xz",
                },
                {k: v for k, v in fc.call_args.kwargs.items() if bool(v) and v is not UNSET},
            )
            spt.assert_called_once()
            self.assertEqual(
                {
                    "older": "2d",
                    "newer": "7d",
                    "top": "/var/log/container",
                    "compress": "xz",
                    "dotenv": True,
                    "s3_secret_key": "mysecretkey",
                },
                {k: v for k, v in spt.call_args.kwargs.items() if bool(v) and v is not UNSET},
            )
            fd.assert_called_once()
            self.assertEqual(
                {"older": "400d", "top": "/var/log/container"},
                {k: v for k, v in fd.call_args.kwargs.items() if bool(v) and v is not UNSET},
            )
