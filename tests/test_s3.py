import unittest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from log2s3.main import cli


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
            res = CliRunner().invoke(cli, ["s3-make-bucket", "--s3-bucket", "mytestbucket123"], env=self.envs)
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                's3', aws_access_key_id="access123", aws_secret_access_key="secret123",
                region_name="region123", endpoint_url="https://example.com/")
            cl.return_value.create_bucket.assert_called_once_with(Bucket="mytestbucket123")

    def test_s3_list_buckets(self):
        with patch("boto3.client") as cl:
            res = CliRunner().invoke(cli, ["s3-bucket"], env=self.envs)
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                's3', aws_access_key_id="access123", aws_secret_access_key="secret123",
                region_name="region123", endpoint_url="https://example.com/")
            cl.return_value.list_buckets.assert_called_once_with()

    def test_s3_list_objects(self):
        with patch("boto3.client") as cl:
            cl.return_value.list_objects.side_effect = [{
                "IsTruncated": True,
                "Contents": [{"LastModified": "1234", "Size": 1234, "Key": "key1234"}],
            }, {
                "IsTruncated": False,
                "Contents": [{"LastModified": "12345", "Size": 12345, "Key": "key12345"}],
            },
            ]
            res = CliRunner().invoke(cli, ["s3-list", "--s3-bucket", "bucket123"], env=self.envs)
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                's3', aws_access_key_id="access123", aws_secret_access_key="secret123",
                region_name="region123", endpoint_url="https://example.com/")
            self.assertEqual(2, cl.return_value.list_objects.call_count)
            self.assertIn("key12345", res.output)

    def test_s3_du(self):
        with patch("boto3.client") as cl:
            cl.return_value.list_objects.side_effect = [{
                "IsTruncated": True,
                "Contents": [
                    {"LastModified": "1234", "Size": 1234, "Key": "dir1/key1234"},
                    {"LastModified": "1234", "Size": 4321, "Key": "dir1/key2345"},
                ],
            }, {
                "IsTruncated": False,
                "Contents": [
                    {"LastModified": "12345", "Size": 4444, "Key": "dir1/key12345"},
                    {"LastModified": "12345", "Size": 12345, "Key": "dir2/key23456"},
                ],
            },
            ]
            res = CliRunner().invoke(cli, ["s3-du", "--s3-bucket", "bucket123"], env=self.envs)
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                's3', aws_access_key_id="access123", aws_secret_access_key="secret123",
                region_name="region123", endpoint_url="https://example.com/")
            self.assertEqual(2, cl.return_value.list_objects.call_count)
            self.assertIn(" 9999 ", res.output)  # 1234 + 4321 + 4444
            self.assertIn(" 3 dir1", res.output)

    def test_s3_du_s(self):
        with patch("boto3.client") as cl:
            cl.return_value.list_objects.side_effect = [{
                "IsTruncated": True,
                "Contents": [
                    {"LastModified": "1234", "Size": 1234, "Key": "dir1/key1234"},
                    {"LastModified": "1234", "Size": 4321, "Key": "dir1/key/2345"},
                ],
            }, {
                "IsTruncated": False,
                "Contents": [
                    {"LastModified": "12345", "Size": 4444, "Key": "dir1/key12345"},
                    {"LastModified": "12345", "Size": 12345, "Key": "dir2/key23456"},
                ],
            },
            ]
            res = CliRunner().invoke(cli, ["s3-du", "--s3-bucket", "bucket123", "-S"], env=self.envs)
            if res.exception:
                raise res.exception
            self.assertEqual(0, res.exit_code)
            cl.assert_called_once_with(
                's3', aws_access_key_id="access123", aws_secret_access_key="secret123",
                region_name="region123", endpoint_url="https://example.com/")
            self.assertEqual(2, cl.return_value.list_objects.call_count)
            self.assertIn(" 9999 ", res.output)  # 1234 + 4321 + 4444
            self.assertIn(" 3 dir1", res.output)
            self.assertIn(" 1 dir1/key", res.output)
