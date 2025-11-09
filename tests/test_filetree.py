import unittest
from datetime import datetime, timedelta
import tempfile
import pathlib
import io
import os
import glob
from click.testing import CliRunner
from log2s3.main import cli


class TestProcessor(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.basedir = pathlib.Path(self.td.name)
        for dir in [f"dir{x}" for x in range(5)]:
            (self.basedir / dir).mkdir()
            for day in range(10):
                dt = datetime.now() - timedelta(days=day)
                sz = (day + 1) * 1024
                logfile = self.basedir / dir / dt.strftime("%Y-%m-%d.log")
                logfile.write_bytes(b"\0" * sz)
                os.utime(logfile, (dt.timestamp(), dt.timestamp()))

    def tearDown(self):
        del self.td

    def test_debug(self):
        with self.assertLogs(level="INFO") as dlog:
            res = CliRunner().invoke(cli, ["filetree-debug", "--top", self.td.name])
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            self.assertEqual(50, len(dlog.records))

    def test_list(self):
        res = CliRunner().invoke(cli, ["filetree-list", "--top", self.td.name])
        if res.exception:
            raise res.exception
        self.assertEqual(0, res.exit_code)
        self.assertEqual(50 + 3, len(res.output.split("\n")))

    def test_list2(self):
        res = CliRunner().invoke(
            cli,
            [
                "filetree-list",
                "--top",
                self.td.name,
                "--date",
                (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
            ],
        )
        if res.exception:
            raise res.exception
        self.assertEqual(0, res.exit_code)
        self.assertEqual(5 + 3, len(res.output.split("\n")))

    def test_compress(self):
        res = CliRunner().invoke(
            cli,
            [
                "filetree-compress",
                "--top",
                self.td.name,
                "--date",
                (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
            ],
        )
        if res.exception:
            raise res.exception
        self.assertEqual(0, res.exit_code)
        self.assertEqual(5, len(glob.glob(os.path.join(self.td.name, "*", "*.gz"))))

    def test_delete(self):
        res = CliRunner().invoke(
            cli,
            [
                "filetree-delete",
                "--top",
                self.td.name,
                "--date",
                (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
            ],
        )
        if res.exception:
            raise res.exception
        self.assertEqual(0, res.exit_code)
        self.assertEqual(50 - 5, len(glob.glob(os.path.join(self.td.name, "*", "*.log"))))

    def test_compbench(self):
        dt = datetime.now() - timedelta(days=2)
        res = CliRunner().invoke(
            cli,
            [
                "compress-benchmark",
                os.path.join(self.td.name, "dir0", dt.strftime("%Y-%m-%d.log")),
            ],
        )
        if res.exception:
            raise res.exception
        self.assertEqual(0, res.exit_code)
        self.assertIn("gzip", res.output)

    def test_compbench2(self):
        dt = datetime.now() - timedelta(days=2)
        res = CliRunner().invoke(
            cli,
            [
                "compress-benchmark",
                "--compress",
                "gzip",
                "--compress",
                "bzip2",
                os.path.join(self.td.name, "dir0", dt.strftime("%Y-%m-%d.log")),
            ],
        )
        if res.exception:
            raise res.exception
        self.assertEqual(0, res.exit_code)
        self.assertIn("gzip", res.output)
        self.assertIn("bzip2", res.output)
        self.assertNotIn("xz", res.output)


class TestMerge(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.basedir = pathlib.Path(self.td.name)
        for dir in [f"dir{x}" for x in range(5)]:
            (self.basedir / dir).mkdir()
            for day in range(10):
                dt = datetime.now() - timedelta(days=day)
                logf = io.StringIO()
                for i in range(1000):
                    ts = dt + timedelta(seconds=59 * i)
                    logf.write(ts.strftime("%Y-%m-%d %H:%M:%S") + " " + dir + " hello world\n")
                logfile = self.basedir / dir / dt.strftime("%Y-%m-%d.log")
                logfile.write_bytes(logf.getvalue().encode("utf-8"))

    def tearDown(self):
        del self.td

    def test_merge_dir(self):
        res = CliRunner().invoke(cli, ["merge", self.td.name, "--verbose"])
        if res.exception:
            raise res.exception
        self.assertIn("hello world", res.output)

    def test_merge_file(self):
        dt = datetime.now() - timedelta(days=1)
        fn1 = pathlib.Path(self.td.name) / "dir0" / dt.strftime("%Y-%m-%d.log")
        fn2 = pathlib.Path(self.td.name) / "dir1" / dt.strftime("%Y-%m-%d.log")
        fn3 = pathlib.Path(self.td.name) / "dir2" / dt.strftime("%Y-%m-%d.log")
        res = CliRunner().invoke(cli, ["merge", str(fn1), str(fn2), str(fn3)])
        if res.exception:
            raise res.exception
        self.assertIn("hello world", res.output)
