import unittest
import tempfile
import datetime
import gzip
from pathlib import Path
from fastapi import FastAPI
from fastapi.testclient import TestClient
from log2s3.app import router, update_config


class TestApp(unittest.TestCase):
    raw_content = "hello\nworld\n"
    gz_content = gzip.compress(raw_content.encode("utf-8"))

    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app)
        self.td = tempfile.TemporaryDirectory()
        update_config({"working_dir": self.td.name})
        for name in ["hello", "world", "foo", "bar", "baz"]:
            dtst = datetime.date(2024, 1, 1)
            dirp = Path(self.td.name) / name
            dirp.mkdir(exist_ok=True)
            for i in range(10):
                basename = (dtst + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                fp = dirp / (basename+".log")
                fp.write_text(self.raw_content)
            dtst = datetime.date(2024, 2, 1)
            for i in range(10, 20):
                basename = (dtst + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                fp = dirp / (basename+".log.gz")
                fp.write_bytes(self.gz_content)

    def tearDown(self):
        del self.client
        del self.app
        del self.td

    def test_config(self):
        res = self.client.get("/config")
        self.assertEqual(200, res.status_code)
        self.assertIn("working_dir", list(res.json()))

    def test_list(self):
        res = self.client.get("/list/")
        self.assertEqual(200, res.status_code)
        self.assertEqual(5, len(res.json()))

    def test_list_prefix(self):
        res = self.client.get("/list/ba")
        self.assertEqual(200, res.status_code)
        self.assertEqual({"bar", "baz"}, set(res.json().keys()))

    def test_list_file(self):
        res = self.client.get("/list/bar/2024-01-02.log")
        self.assertEqual(200, res.status_code)
        self.assertEqual({"bar": {"2024-01-02": "bar/2024-01-02.log"}}, res.json())

    def test_list_year(self):
        res = self.client.get("/list/foo", params={"month": "2024-"})
        self.assertEqual(200, res.status_code)
        self.assertEqual({
            "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
            "2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10",
            "2024-02-11", "2024-02-12", "2024-02-13", "2024-02-14", "2024-02-15",
            "2024-02-16", "2024-02-17", "2024-02-18", "2024-02-19", "2024-02-20",
        },
            set(res.json()["foo"].keys()))

    def test_list_month(self):
        res = self.client.get("/list/baz", params={"month": "2024-01"})
        self.assertEqual(200, res.status_code)
        self.assertEqual({
            "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
            "2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10"},
            set(res.json()["baz"].keys()))

    def test_read_raw(self):
        res = self.client.get("/read/baz/2024-01-01.log")
        self.assertEqual(200, res.status_code)
        self.assertEqual(self.raw_content, res.text)

    def test_read_decompress(self):
        res = self.client.get("/read/baz/2024-02-11.log", headers={"accept-encoding": "raw"})
        self.assertEqual(200, res.status_code)
        self.assertEqual(self.raw_content, res.text)

    def test_read_compressed(self):
        res = self.client.get("/read/baz/2024-02-11.log", headers={"accept-encoding": "gzip, br"})
        self.assertEqual(200, res.status_code)
        self.assertEqual(self.gz_content, res.content)

    def test_read_dir(self):
        res = self.client.get("/read/baz", headers={"accept-encoding": "gzip, br"})
        self.assertEqual(403, res.status_code)

    def test_read_notfound(self):
        res = self.client.get("/read/baz/2099-02-11.log", headers={"accept-encoding": "gzip, br"})
        self.assertEqual(404, res.status_code)

    def test_read_html1(self):
        res = self.client.get("/html1/ba")
        self.assertEqual(200, res.status_code)
        self.assertIn("text/html", res.headers.get("content-type"))
        self.assertIn("2024-01-04", res.text)
        self.assertIn("2024-02-14", res.text)

    def test_read_html1_month(self):
        res = self.client.get("/html1/ba", params={"month": "2024-01"})
        self.assertEqual(200, res.status_code)
        self.assertIn("text/html", res.headers.get("content-type"))
        self.assertIn("2024-01-04", res.text)
        self.assertNotIn("2024-02", res.text)

    def test_read_html1_notfound(self):
        res = self.client.get("/html1/ba", params={"month": "2025-01"})
        self.assertEqual(404, res.status_code)

    def test_read_html2(self):
        res = self.client.get("/html2/ba")
        self.assertEqual(200, res.status_code)
        self.assertIn("text/html", res.headers.get("content-type"))
        self.assertIn("2024-01-04", res.text)
        self.assertIn("2024-02-14", res.text)

    def test_read_html2_month(self):
        res = self.client.get("/html2/ba", params={"month": "2024-01"})
        self.assertEqual(200, res.status_code)
        self.assertIn("text/html", res.headers.get("content-type"))
        self.assertIn("2024-01-04", res.text)
        self.assertNotIn("2024-02", res.text)

    def test_read_html2_notfound(self):
        res = self.client.get("/html2/ba", params={"month": "2025-01"})
        self.assertEqual(404, res.status_code)

    def test_cat_month(self):
        res = self.client.get("/cat/ba", params={"month": "2024-01"})
        self.assertEqual(200, res.status_code)
        self.assertIn("text/plain", res.headers.get("content-type"))
        self.assertIn(self.raw_content, res.text)

    def test_cat_month_notfound(self):
        res = self.client.get("/cat/ba", params={"month": "2025-01"})
        self.assertEqual(404, res.status_code)

    def test_merge_month(self):
        res = self.client.get("/merge/ba", params={"month": "2024-01"})
        self.assertEqual(200, res.status_code)
        self.assertIn("text/plain", res.headers.get("content-type"))
        self.assertIn(self.raw_content, res.text)

    def test_merge_month_notfound(self):
        res = self.client.get("/merge/ba", params={"month": "2025-01"})
        self.assertEqual(404, res.status_code)

    def test_read_link(self):
        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            (Path(self.td.name) / "testlink").symlink_to(tdp)
            (tdp / "2024-01-01.log").write_text("cannot read\n")
            res = self.client.get("/read/testlink/2024-01-01.log")
            self.assertEqual(403, res.status_code)

    def test_list_link(self):
        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            (Path(self.td.name) / "testlink").symlink_to(tdp)
            (tdp / "2024-01-01.log").write_text("cannot read\n")
            res = self.client.get("/list/")
            self.assertEqual(200, res.status_code)
            self.assertNotIn("testlink", res.json())

    def test_list_link2(self):
        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            (Path(self.td.name) / "testlink").symlink_to(tdp)
            (tdp / "2024-01-01.log").write_text("cannot read\n")
            res = self.client.get("/list/test")
            self.assertEqual(200, res.status_code)
            self.assertNotIn("testlink", res.json())
