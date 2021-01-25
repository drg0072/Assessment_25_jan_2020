import unittest
from pathlib import Path

import mock
import requests_mock


def check_upload_to_aws_s3(csv_path, s3_bucket):
    assert isinstance(csv_path, Path)
    return True


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        import os
        os.makedirs("test", exist_ok=True)
        os.makedirs("test/tmp", exist_ok=True)

    @requests_mock.mock()
    def test_download_file(self, mock_requests):
        # mocking your request(s)
        expected_headers = {
            'content-disposition': "filename=ABC.xml",
            'content-type': "application/xml"
        }
        with open(Path("test/resource/test.xml"), 'r') as fp:
            data = fp.read().encode()
        mock_requests.get("http://test.com/XYZ", headers=expected_headers, content=data, status_code=200)

        from main_xml_parser import download_file

        file = download_file("http://test.com/XYZ", "test/tmp/", "ABC")
        op_file = Path("test/tmp/ABC.xml")
        assert op_file.is_file()
        assert isinstance(file, Path)
        assert file.is_file()
        with open(op_file, "r+") as fp, open(Path("test/resource/test.xml"), 'r') as fp_test:
            assert fp.read() == fp_test.read()

    def test_main_xml(self):
        xml_path = Path("test/resource/test.xml")

        from main_xml_parser import parse_main_xml
        link = parse_main_xml(xml_path)

        assert link == "http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip"

    @mock.patch('main_xml_parser.upload_to_aws_s3', new=check_upload_to_aws_s3)
    @requests_mock.mock()
    def test_main(self, mock_requests):
        expected_headers = {
            'content-disposition': "filename=ABC.xml",
            'content-type': "application/xml"
        }
        with open(Path("test/resource/test.xml"), 'r') as fp:
            data = fp.read().encode()
        mock_requests.get("http://test.com/XYZ", headers=expected_headers, content=data, status_code=200)

        with open(Path("test/resource/main.zip"), 'rb') as fp:
            zip_data = fp.read()

        mock_requests.get("http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip",
                          headers={
                              'content-type': "application/zip"
                          }, content=zip_data, status_code=200)

        import main_xml_parser
        main_xml_parser.TMP_DIR = "test/tmp"

        file = main_xml_parser.main("http://test.com/XYZ")
        op_file = Path("test/tmp/main.csv")
        assert op_file.is_file()
        assert isinstance(file, Path)
        assert file.is_file()
        with open(op_file, "r+") as fp, open(Path("test/resource/main.csv"), 'r') as fp_test:
            assert fp.read() == fp_test.read()


if __name__ == '__main__':
    unittest.main()
