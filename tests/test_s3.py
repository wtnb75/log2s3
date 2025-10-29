import unittest
import datetime
from click.testing import CliRunner
from unittest.mock import patch, MagicMock, ANY
from log2s3.main import cli

now = datetime.datetime.now()


class TestS3(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    envs = {
        "AWS_ACCESS_KEY_ID": "access123",
        "AWS_SECRET_ACCESS_KEY": "secret123",
        "AWS_DEFAULT_REGION": "region123",
        "AWS_ENDPOINT_URL_S3": "https://example.com/",
    }

    def test_s3_make_buckets(self):
        with patch("boto3.client") as cl:
            res = CliRunner().invoke(
                cli, ["s3-make-bucket", "--s3-bucket", "mytestbucket123"], env=self.envs
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                "s3",
                aws_access_key_id="access123",
                aws_secret_access_key="secret123",
                region_name="region123",
                endpoint_url="https://example.com/",
            )
            cl.return_value.create_bucket.assert_called_once_with(
                Bucket="mytestbucket123"
            )

    def test_s3_list_buckets(self):
        with patch("boto3.client") as cl:
            res = CliRunner().invoke(cli, ["s3-bucket"], env=self.envs)
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                "s3",
                aws_access_key_id="access123",
                aws_secret_access_key="secret123",
                region_name="region123",
                endpoint_url="https://example.com/",
            )
            cl.return_value.list_buckets.assert_called_once_with()

    def test_s3_list_objects(self):
        with patch("boto3.client") as cl:
            cl.return_value.list_objects.side_effect = [
                {
                    "IsTruncated": True,
                    "Contents": [{"LastModified": now, "Size": 1234, "Key": "key1234"}],
                },
                {
                    "IsTruncated": False,
                    "Contents": [
                        {"LastModified": now, "Size": 12345, "Key": "key12345"}
                    ],
                },
            ]
            res = CliRunner().invoke(
                cli, ["s3-list", "--s3-bucket", "bucket123"], env=self.envs
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                "s3",
                aws_access_key_id="access123",
                aws_secret_access_key="secret123",
                region_name="region123",
                endpoint_url="https://example.com/",
            )
            self.assertEqual(2, cl.return_value.list_objects.call_count)
            self.assertIn("key12345", res.output)

    def test_s3_du(self):
        with patch("boto3.client") as cl:
            cl.return_value.list_objects.side_effect = [
                {
                    "IsTruncated": True,
                    "Contents": [
                        {"LastModified": now, "Size": 1234, "Key": "dir1/key1234"},
                        {"LastModified": now, "Size": 4321, "Key": "dir1/key2345"},
                    ],
                },
                {
                    "IsTruncated": False,
                    "Contents": [
                        {"LastModified": now, "Size": 4444, "Key": "dir1/key12345"},
                        {"LastModified": now, "Size": 12345, "Key": "dir2/key23456"},
                    ],
                },
            ]
            res = CliRunner().invoke(
                cli, ["s3-du", "--s3-bucket", "bucket123"], env=self.envs
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                "s3",
                aws_access_key_id="access123",
                aws_secret_access_key="secret123",
                region_name="region123",
                endpoint_url="https://example.com/",
            )
            self.assertEqual(2, cl.return_value.list_objects.call_count)
            self.assertIn(" 9999 ", res.output)  # 1234 + 4321 + 4444
            self.assertIn(" 3 dir1", res.output)

    def test_s3_du_empty(self):
        with patch("boto3.client") as cl:
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": [],
            }
            res = CliRunner().invoke(
                cli, ["s3-du", "--s3-bucket", "bucket123"], env=self.envs
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                "s3",
                aws_access_key_id="access123",
                aws_secret_access_key="secret123",
                region_name="region123",
                endpoint_url="https://example.com/",
            )
            self.assertIn("empty result", res.output)

    def test_s3_du_s(self):
        with patch("boto3.client") as cl:
            cl.return_value.list_objects.side_effect = [
                {
                    "IsTruncated": True,
                    "Contents": [
                        {"LastModified": now, "Size": 1234, "Key": "dir1/key1234"},
                        {"LastModified": now, "Size": 4321, "Key": "dir1/key/2345"},
                    ],
                },
                {
                    "IsTruncated": False,
                    "Contents": [
                        {"LastModified": now, "Size": 4444, "Key": "dir1/key12345"},
                        {"LastModified": now, "Size": 12345, "Key": "dir2/key23456"},
                    ],
                },
            ]
            res = CliRunner().invoke(
                cli, ["s3-du", "--s3-bucket", "bucket123", "-S"], env=self.envs
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                "s3",
                aws_access_key_id="access123",
                aws_secret_access_key="secret123",
                region_name="region123",
                endpoint_url="https://example.com/",
            )
            self.assertEqual(2, cl.return_value.list_objects.call_count)
            self.assertIn(" 9999 ", res.output)  # 1234 + 4321 + 4444
            self.assertIn(" 3 dir1", res.output)
            self.assertIn(" 1 dir1/key", res.output)

    def test_s3_del_by_all(self):
        with patch("boto3.client") as cl:
            from datetime import datetime, timedelta

            now = datetime.now()
            c = [
                {"LastModified": now - timedelta(x), "Size": x * 1000, "Key": f"obj{x}"}
                for x in range(10)
            ]
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": c,
            }
            res = CliRunner().invoke(
                cli, ["s3-delete-by", "--s3-bucket", "bucket123"], env=self.envs
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.delete_objects.assert_called_once_with(
                Bucket="bucket123",
                Delete={"Objects": [{"Key": f"obj{x}"} for x in range(10)]},
            )

    def test_s3_del_by_older(self):
        with patch("boto3.client") as cl:
            from datetime import datetime, timedelta

            now = datetime.now()
            c = [
                {"LastModified": now - timedelta(x), "Size": x * 1000, "Key": f"obj{x}"}
                for x in range(10)
            ]
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": c,
            }
            res = CliRunner().invoke(
                cli,
                ["s3-delete-by", "--s3-bucket", "bucket123", "--older", "3d"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.delete_objects.assert_called_once_with(
                Bucket="bucket123",
                Delete={"Objects": [{"Key": f"obj{x}"} for x in range(3, 10)]},
            )

    def test_s3_del_by_older_dry(self):
        with patch("boto3.client") as cl:
            from datetime import datetime, timedelta

            now = datetime.now()
            c = [
                {"LastModified": now - timedelta(x), "Size": x * 1000, "Key": f"obj{x}"}
                for x in range(10)
            ]
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": c,
            }
            res = CliRunner().invoke(
                cli,
                ["s3-delete-by", "--s3-bucket", "bucket123", "--older", "3d", "--dry"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.delete_objects.assert_not_called()
            self.assertIn("dry", res.output)

    def test_s3_del_by_newer(self):
        with patch("boto3.client") as cl:
            from datetime import datetime, timedelta

            now = datetime.now()
            c = [
                {"LastModified": now - timedelta(x), "Size": x * 1000, "Key": f"obj{x}"}
                for x in range(10)
            ]
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": c,
            }
            res = CliRunner().invoke(
                cli,
                ["s3-delete-by", "--s3-bucket", "bucket123", "--newer", "3d"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.delete_objects.assert_called_once_with(
                Bucket="bucket123",
                Delete={"Objects": [{"Key": f"obj{x}"} for x in range(3)]},
            )

    def test_s3_del_by_older_newer(self):
        with patch("boto3.client") as cl:
            from datetime import datetime, timedelta

            now = datetime.now()
            c = [
                {"LastModified": now - timedelta(x), "Size": x * 1000, "Key": f"obj{x}"}
                for x in range(10)
            ]
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": c,
            }
            res = CliRunner().invoke(
                cli,
                [
                    "s3-delete-by",
                    "--s3-bucket",
                    "bucket123",
                    "--newer",
                    "7d",
                    "--older",
                    "2d",
                ],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.delete_objects.assert_called_once_with(
                Bucket="bucket123",
                Delete={"Objects": [{"Key": f"obj{x}"} for x in range(2, 7)]},
            )

    def test_s3_del_by_bigger(self):
        with patch("boto3.client") as cl:
            from datetime import datetime, timedelta

            now = datetime.now()
            c = [
                {"LastModified": now - timedelta(x), "Size": x * 1000, "Key": f"obj{x}"}
                for x in range(10)
            ]
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": c,
            }
            res = CliRunner().invoke(
                cli,
                ["s3-delete-by", "--s3-bucket", "bucket123", "--bigger", "4k"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.delete_objects.assert_called_once_with(
                Bucket="bucket123",
                Delete={"Objects": [{"Key": f"obj{x}"} for x in range(5, 10)]},
            )

    def test_s3_del_by_smaller(self):
        with patch("boto3.client") as cl:
            from datetime import datetime, timedelta

            now = datetime.now()
            c = [
                {"LastModified": now - timedelta(x), "Size": x * 1000, "Key": f"obj{x}"}
                for x in range(10)
            ]
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": c,
            }
            res = CliRunner().invoke(
                cli,
                ["s3-delete-by", "--s3-bucket", "bucket123", "--smaller", "4k"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.delete_objects.assert_called_once_with(
                Bucket="bucket123",
                Delete={"Objects": [{"Key": f"obj{x}"} for x in range(5)]},
            )

    def test_s3_del_by_suffix(self):
        with patch("boto3.client") as cl:
            from datetime import datetime, timedelta

            now = datetime.now()
            c = [
                {"LastModified": now - timedelta(x), "Size": x * 1000, "Key": f"obj{x}"}
                for x in range(11)
            ]
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": c,
            }
            res = CliRunner().invoke(
                cli,
                ["s3-delete-by", "--s3-bucket", "bucket123", "--suffix", "0"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.delete_objects.assert_called_once_with(
                Bucket="bucket123",
                Delete={"Objects": [{"Key": "obj0"}, {"Key": "obj10"}]},
            )

    def test_s3_del_by_notfound(self):
        with patch("boto3.client") as cl:
            from datetime import datetime, timedelta

            now = datetime.now()
            c = [
                {"LastModified": now - timedelta(x), "Size": x * 1000, "Key": f"obj{x}"}
                for x in range(11)
            ]
            cl.return_value.list_objects.return_value = {
                "IsTruncated": False,
                "Contents": c,
            }
            with self.assertLogs(level="INFO") as alog:
                res = CliRunner().invoke(
                    cli,
                    ["s3-delete-by", "--s3-bucket", "bucket123", "--older", "20d"],
                    env=self.envs,
                )
                if res.exception:
                    raise res.exception
                self.assertEqual(0, res.exit_code)
                cl.return_value.delete_objects.assert_not_called()
                self.assertIn("no object found", "\n".join(alog.output))

    def test_s3_cat_gz(self):
        bindata = b"hello world\n" * 1024
        import gzip

        data = gzip.compress(bindata)
        read_mock = MagicMock()
        read_mock.iter_chunks.return_value = [data]
        with patch("boto3.client") as cl:
            cl.return_value.get_object.return_value = {
                "Body": read_mock,
            }
            res = CliRunner().invoke(
                cli,
                ["s3-cat", "--s3-bucket", "bucket123", "path/to/hello.gz"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            self.assertEqual(bindata.decode("utf-8"), res.output)

    def test_s3_less_bz2(self):
        bindata = b"hello world\n" * 1024
        import bz2
        from subprocess import PIPE

        data = bz2.compress(bindata)
        read_mock = MagicMock()
        mid = int(len(data) / 2)
        read_mock.iter_chunks.return_value = [data[:mid], data[mid:]]
        with patch("boto3.client") as cl, patch("subprocess.Popen") as sp:
            cl.return_value.get_object.return_value = {
                "Body": read_mock,
            }
            res = CliRunner().invoke(
                cli,
                ["s3-less", "--s3-bucket", "bucket123", "path/to/hello.bz2"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            sp.assert_called_once_with(["less"], stdin=PIPE)
            self.assertEqual(0, res.exit_code)

    def test_s3_vi_bz2(self):
        bindata = b"hello world\n" * 1024
        moddata = "goodbye world\n" * 100
        import bz2

        data = bz2.compress(bindata)
        read_mock = MagicMock()
        read_mock.iter_chunks.return_value = [data]
        with patch("boto3.client") as cl, patch("click.edit") as ed:
            cl.return_value.get_object.return_value = {
                "Body": read_mock,
            }
            ed.return_value = moddata
            res = CliRunner().invoke(
                cli,
                ["s3-vi", "--s3-bucket", "bucket123", "path/to/hello.bz2"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.put_object.assert_called_once_with(
                Bucket="bucket123", Key="path/to/hello.bz2", Body=ANY
            )
            body = bz2.decompress(cl.return_value.put_object.call_args.kwargs["Body"])
            self.assertEqual(moddata, body.decode("utf-8"))

    def test_s3_vi_xz_unchanged(self):
        bindata = b"hello world\n" * 1024
        moddata = bindata.decode("utf-8")
        import lzma

        data = lzma.compress(bindata, lzma.FORMAT_XZ)
        read_mock = MagicMock()
        read_mock.iter_chunks.return_value = [data]
        with (
            patch("boto3.client") as cl,
            patch("click.edit") as ed,
            self.assertLogs(level="INFO") as alog,
        ):
            cl.return_value.get_object.return_value = {
                "Body": read_mock,
            }
            ed.return_value = moddata
            res = CliRunner().invoke(
                cli,
                ["s3-vi", "--s3-bucket", "bucket123", "path/to/hello.xz"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.put_object.assert_not_called()
            self.assertIn("not changed", "\n".join(alog.output))

    def test_s3_vi_changed_dry(self):
        bindata = b"hello world\n" * 1024
        moddata = "goodbye world\n" * 100
        read_mock = MagicMock()
        read_mock.iter_chunks.return_value = [bindata]
        with (
            patch("boto3.client") as cl,
            patch("click.edit") as ed,
            self.assertLogs(level="INFO") as alog,
        ):
            cl.return_value.get_object.return_value = {
                "Body": read_mock,
            }
            ed.return_value = moddata
            res = CliRunner().invoke(
                cli,
                ["s3-vi", "--s3-bucket", "bucket123", "path/to/hello.log", "--dry"],
                env=self.envs,
            )
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.return_value.put_object.assert_not_called()
            self.assertIn("(dry) changed", "\n".join(alog.output))
