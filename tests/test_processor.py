import unittest
from datetime import datetime, timedelta
import tempfile
import pathlib
import os
from log2s3.processor import process_walk, DelProcessor, CompressProcessor


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

    def _count(self) -> tuple[int, int]:
        cnt = 0
        sz = 0
        for root, _, files in os.walk(self.basedir):
            cnt += len(files)
            sz += sum([pathlib.Path(root, x).stat().st_size for x in files])
        return cnt, sz

    def test_delete1(self):
        dp = DelProcessor({"older": "2d", "bigger": "1k"})
        process_walk(self.basedir, [dp])
        self.assertEqual(10, self._count()[0])  # 2 files x 5 dirs

    def test_delete2(self):
        dp = DelProcessor({"older": "1d", "bigger": "4.1k"})
        process_walk(self.basedir, [dp])
        self.assertEqual(20, self._count()[0])  # 4 files x 5 dirs

    def test_delete3(self):
        pre_cnts = self._count()
        dt = datetime.now()-timedelta(days=2)

        dp = DelProcessor({"date": dt.strftime("%Y-%m-%d")})
        process_walk(self.basedir, [dp])
        cnts = self._count()
        self.assertEqual(pre_cnts[0]-5, cnts[0])

    def test_delete4(self):
        pre_cnts = self._count()
        dt = datetime.now()-timedelta(days=3)
        dt2 = datetime.now()-timedelta(days=1)

        dp = DelProcessor({"date": dt.strftime("%Y-%m-%d")+".."+dt2.strftime("%Y-%m-%d")})
        process_walk(self.basedir, [dp])
        cnts = self._count()
        self.assertEqual(pre_cnts[0]-10, cnts[0])

    def test_delete5(self):
        dp = DelProcessor({"older": "1d", "bigger": "4.1k", "dry": True})
        process_walk(self.basedir, [dp])
        self.assertEqual(50, self._count()[0])

    def test_compress1(self):
        pre_cnts = self._count()
        dp = CompressProcessor({"older": "2d", "bigger": "1k", "compress": "gzip"})
        process_walk(self.basedir, [dp])
        cnts = self._count()
        self.assertEqual(pre_cnts[0], cnts[0])
        self.assertGreater(pre_cnts[1], cnts[1])

    def test_compress1_dry(self):
        pre_cnts = self._count()
        dp = CompressProcessor({"older": "2d", "bigger": "1k", "compress": "gzip", "dry": True})
        process_walk(self.basedir, [dp])
        cnts = self._count()
        self.assertEqual(pre_cnts, cnts)

    def test_compress2(self):
        pre_cnts = self._count()
        dp = CompressProcessor({"bigger": "100k", "compress": "gzip"})
        process_walk(self.basedir, [dp])
        cnts = self._count()
        self.assertEqual(pre_cnts, cnts)  # do nothing
