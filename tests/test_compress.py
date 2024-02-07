import unittest
import pathlib
import tempfile
import subprocess
from log2s3.compr import extcmp_map, auto_compress, do_chain


class TestCompr(unittest.TestCase):
    def test_compdecomp(self):
        input_bytes = b'abcdefg1234567' * 100
        for k, v in extcmp_map.items():
            with self.subTest(f"ext={k} mode={v[0]}"):
                compdata = v[2](input_bytes)
                restored = v[1](compdata)
                self.assertEqual(input_bytes, restored)

    def test_autocomp_decompress(self):
        input_bytes = b'abcdefg1234567' * 100
        with tempfile.TemporaryDirectory() as td:
            tf1 = pathlib.Path(td, "example.data")
            tf2 = pathlib.Path(td, "example.data.gz")
            tf1.write_bytes(input_bytes)
            res = subprocess.run(["gzip", "-f", tf1])
            self.assertEqual(0, res.returncode)
            self.assertTrue(tf2.exists())
            p1, ls = auto_compress(tf2, "decompress")
            self.assertEqual("example.data", pathlib.Path(p1).name)
            self.assertEqual(input_bytes, do_chain(ls))

    def test_autocomp_recompress(self):
        input_bytes = b'abcdefg1234567' * 100
        with tempfile.TemporaryDirectory() as td:
            tf1 = pathlib.Path(td, "example.data")
            tf2 = pathlib.Path(td, "example.data.gz")
            tf1.write_bytes(input_bytes)
            res = subprocess.run(["gzip", "-f", tf1])
            self.assertEqual(0, res.returncode)
            self.assertTrue(tf2.exists())
            p1, ls = auto_compress(tf2, "bzip2")
            self.assertEqual("example.data.bz2", pathlib.Path(p1).name)
            output_bytes = do_chain(ls)
            self.assertNotEqual(input_bytes, output_bytes)
            self.assertNotEqual(tf2.read_bytes(), output_bytes)

    def test_autocomp_unchange(self):
        input_bytes = b'abcdefg1234567' * 100
        with tempfile.TemporaryDirectory() as td:
            tf1 = pathlib.Path(td, "example.data")
            tf2 = pathlib.Path(td, "example.data.gz")
            tf1.write_bytes(input_bytes)
            res = subprocess.run(["gzip", "-f", tf1])
            self.assertEqual(0, res.returncode)
            self.assertTrue(tf2.exists())
            p1, ls = auto_compress(tf2, "gzip")
            self.assertEqual("example.data.gz", pathlib.Path(p1).name)
            self.assertEqual(tf2.read_bytes(), do_chain(ls))

    def test_autocomp_raw(self):
        input_bytes = b'abcdefg1234567' * 100
        with tempfile.TemporaryDirectory() as td:
            tf1 = pathlib.Path(td, "example.data")
            tf2 = pathlib.Path(td, "example.data.gz")
            tf1.write_bytes(input_bytes)
            res = subprocess.run(["gzip", "-f", tf1])
            self.assertEqual(0, res.returncode)
            self.assertTrue(tf2.exists())
            p1, ls = auto_compress(tf2, None)  # == "raw"
            self.assertEqual("example.data.gz", pathlib.Path(p1).name)
            self.assertEqual(tf2.read_bytes(), do_chain(ls))

    def test_autocomp_unkonwn(self):
        input_bytes = b'abcdefg1234567' * 100
        with tempfile.TemporaryDirectory() as td:
            tf1 = pathlib.Path(td, "example.data")
            tf2 = pathlib.Path(td, "example.data.gz")
            tf1.write_bytes(input_bytes)
            res = subprocess.run(["gzip", "-f", tf1])
            self.assertEqual(0, res.returncode)
            self.assertTrue(tf2.exists())
            p1, ls = auto_compress(tf2, "invalid")  # == "decompress"
            self.assertEqual("example.data.gz", pathlib.Path(p1).name)
            self.assertEqual(input_bytes, do_chain(ls))
