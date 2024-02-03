import unittest
from log2s3.compr import extcmp_map


class TestCompr(unittest.TestCase):
    def test_compdecomp(self):
        input_bytes = b'abcdefg1234567' * 100
        for k, v in extcmp_map.items():
            with self.subTest(f"ext={k} mode={v[0]}"):
                compdata = v[2](input_bytes)
                restored = v[1](compdata)
                self.assertEqual(input_bytes, restored)
