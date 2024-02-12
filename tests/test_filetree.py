import unittest
from datetime import datetime, timedelta
import tempfile
import pathlib
import os
import glob
from click.testing import CliRunner
from unittest.mock import patch, MagicMock, ANY
from log2s3.main import cli


class TestProcessor(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.basedir = pathlib.Path(self.td.name)
        for dir in [f"dir{x}" for x in range(5)]:
            (self.basedir / dir).mkdir()
            for day in range(10):
                dt = datetime.now()-timedelta(days=day)
                sz = (day+1) * 1024
                logfile = (self.basedir / dir / dt.strftime("%Y-%m-%d.log"))
                logfile.write_bytes(b'\0'*sz)
                os.utime(logfile, (dt.timestamp(), dt.timestamp()))

    def tearDown(self):
        del self.td

    def test_debug(self):
        with self.assertLogs(level="INFO") as dlog:
            res = CliRunner().invoke(cli, ["filetree-debug", "--top", self.td.name])
            if res.exception:
                raise res.exceptoin
            self.assertEqual(0, res.exit_code)
            self.assertEqual(50, len(dlog.records))

    def test_list(self):
        res = CliRunner().invoke(cli, ["filetree-list", "--top", self.td.name])
        if res.exception:
            raise res.exceptoin
        self.assertEqual(0, res.exit_code)
        self.assertEqual(50+3, len(res.output.split("\n")))

    def test_list2(self):
        res = CliRunner().invoke(cli, ["filetree-list", "--top", self.td.name, "--date",
                                       (datetime.now()-timedelta(days=2)).strftime("%Y-%m-%d")])
        if res.exception:
            raise res.exceptoin
        self.assertEqual(0, res.exit_code)
        self.assertEqual(5+3, len(res.output.split("\n")))

    def test_compress(self):
        res = CliRunner().invoke(cli, ["filetree-compress", "--top", self.td.name, "--date",
                                       (datetime.now()-timedelta(days=2)).strftime("%Y-%m-%d")])
        if res.exception:
            raise res.exceptoin
        self.assertEqual(0, res.exit_code)
        self.assertEqual(5, len(glob.glob(os.path.join(self.td.name, "*", "*.gz"))))

    def test_delete(self):
        res = CliRunner().invoke(cli, ["filetree-delete", "--top", self.td.name, "--date",
                                       (datetime.now()-timedelta(days=2)).strftime("%Y-%m-%d")])
        if res.exception:
            raise res.exceptoin
        self.assertEqual(0, res.exit_code)
        self.assertEqual(50-5, len(glob.glob(os.path.join(self.td.name, "*", "*.log"))))
