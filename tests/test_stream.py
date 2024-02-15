import unittest
import tempfile
from log2s3.compr_stream import FileReadStream, FileWriteStream, stream_map


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
