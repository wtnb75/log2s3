import unittest
import tempfile
import lzma
import gzip
import pathlib
from log2s3.compr_stream import FileReadStream, FileWriteStream, \
    RawReadStream, stream_map, auto_compress_stream


class TestInOut(unittest.TestCase):
    input_data = b"hello world\n"*1000 + b"rest"
    text_output = input_data.decode("utf-8").splitlines(keepends=True)

    def setUp(self):
        self.tf = tempfile.NamedTemporaryFile("r+b")
        self.tf.write(self.input_data)
        self.tf.flush()
        self.tf.seek(0)

    def tearDown(self):
        del self.tf

    def test_stream(self):
        for k, v in stream_map.items():
            ext, compr, decompr = v
            self.tf.seek(0)
            with self.subTest(f"in/out {k} / *{ext}"):
                prev = FileReadStream(self.tf, 3)
                comp = compr(prev)
                decomp = decompr(comp)
                self.assertEqual(self.text_output, list(decomp.text_gen()))

    def test_write(self):
        for k, v in stream_map.items():
            ext, compr, decompr = v
            self.tf.seek(0)
            with self.subTest(f"in/out {k} / *{ext}"):
                prev = FileReadStream(self.tf, 3)
                comp = compr(prev)
                decomp = decompr(comp)
                with tempfile.TemporaryFile("w+b") as tfout:
                    wr = FileWriteStream(decomp, tfout)
                    for _ in wr.gen():
                        pass
                    tfout.flush()
                    tfout.seek(0)
                    self.assertEqual(self.input_data, tfout.read())


class TestAutoStream(unittest.TestCase):
    def test_xz2decompress(self):
        data = b"hello world\n" * 1000
        xzdata = lzma.compress(data, lzma.FORMAT_XZ)
        with tempfile.TemporaryFile("w+b", suffix=".xz") as tf:
            tf.write(xzdata)
            tf.flush()
            tf.seek(0)
            st = FileReadStream(tf)
            name, cst = auto_compress_stream(pathlib.Path("hello.xz"), "decompress", st)
            self.assertEqual(data, cst.read_all())
            self.assertEqual("hello", str(name))

    def test_xz2decompress_file(self):
        data = b"hello world\n" * 1000
        xzdata = lzma.compress(data, lzma.FORMAT_XZ)
        with tempfile.NamedTemporaryFile("w+b", suffix=".xz") as tf:
            tf.write(xzdata)
            tf.flush()
            tf.seek(0)
            name, cst = auto_compress_stream(pathlib.Path(tf.name), "decompress")
            self.assertEqual(data, cst.read_all())
            self.assertEqual(str(name), tf.name.rsplit(".", 1)[0])

    def test_xz2decompress_bytes(self):
        data = b"hello world\n" * 1000
        xzdata = lzma.compress(data, lzma.FORMAT_XZ)
        st = RawReadStream(xzdata)
        name, cst = auto_compress_stream(pathlib.Path("test.xz"), "decompress", st)
        self.assertEqual(data, cst.read_all())
        self.assertEqual("test", str(name))

    def test_xz2unknown_file(self):
        data = b"hello world\n" * 1000
        xzdata = lzma.compress(data, lzma.FORMAT_XZ)
        with tempfile.NamedTemporaryFile("w+b", suffix=".xz") as tf:
            tf.write(xzdata)
            tf.flush()
            tf.seek(0)
            name, cst = auto_compress_stream(pathlib.Path(tf.name), "unknown")
            self.assertEqual(data, cst.read_all())
            self.assertEqual(tf.name.rsplit(".")[0], str(name))

    def test_unknown2xz_file(self):
        data = b"hello world\n" * 1000
        xzdata = lzma.compress(data, lzma.FORMAT_XZ)
        with tempfile.NamedTemporaryFile("w+b", suffix=".unkonwn") as tf:
            tf.write(data)
            tf.flush()
            tf.seek(0)
            name, cst = auto_compress_stream(pathlib.Path(tf.name), "xz")
            self.assertEqual(xzdata, cst.read_all())
            self.assertEqual(tf.name+".xz", str(name))

    def test_xz2gzip(self):
        data = b"hello world\n" * 1000
        xzdata = lzma.compress(data, lzma.FORMAT_XZ)
        with tempfile.TemporaryFile("w+b", suffix=".xz") as tf:
            tf.write(xzdata)
            tf.flush()
            tf.seek(0)
            st = FileReadStream(tf)
            name, cst = auto_compress_stream(pathlib.Path("hello.xz"), "gzip", st)
            self.assertEqual(data, gzip.decompress(cst.read_all()))
            self.assertEqual("hello.gz", str(name))

    def test_xzraw(self):
        data = b"hello world\n" * 1000
        xzdata = lzma.compress(data, lzma.FORMAT_XZ)
        with tempfile.TemporaryFile("w+b", suffix=".xz") as tf:
            tf.write(xzdata)
            tf.flush()
            tf.seek(0)
            st = FileReadStream(tf)
            name, cst = auto_compress_stream(pathlib.Path("hello.xz"), "raw", st)
            self.assertEqual(xzdata, cst.read_all())
            self.assertEqual("hello.xz", str(name))

    def test_xz2xz(self):
        data = b"hello world\n" * 1000
        xzdata = lzma.compress(data, lzma.FORMAT_XZ)
        with tempfile.TemporaryFile("w+b", suffix=".xz") as tf:
            tf.write(xzdata)
            tf.flush()
            tf.seek(0)
            st = FileReadStream(tf)
            name, cst = auto_compress_stream(pathlib.Path("hello.xz"), "xz", st)
            self.assertEqual(xzdata, cst.read_all())
            self.assertEqual(str(name), "hello.xz")
