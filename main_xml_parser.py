# Global import
import logging
import os
import re
from csv import DictWriter
from pathlib import Path
from typing import Union, List
import boto3
import requests

# Local import

# Global Variables
LOGGER = "MAIN"
TMP_DIR = "tmp"


# Main Code


def download_file(link: str, save_dir: str, file_name: str = 'tmp') -> Union[Path, None]:
    """
        Download the file from Link to the Save Dir.
    :param file_name: File name to be store
    :param link: Link of the Web File.
    :param save_dir: Dir where the file needs to be save
    :return: File PAth
    """
    logging.getLogger(LOGGER).debug(f"Request to Download file {link}")
    try:
        r = requests.request("GET", link, verify=False, stream=True)
    except requests.ConnectionError:
        logging.getLogger(LOGGER).error(f"Link({link}) connection failed.")
        return None

    if 'content-disposition' in r.headers:
        if len(re.findall(r"filename=(.+)", r.headers.get('content-disposition'))) > 0:
            file_name = re.findall(r"filename=(.+)", r.headers.get('content-disposition'))[0]

    file_type = ""
    if 'content-Type' in r.headers:
        if len(re.findall(r"application/(.+);", r.headers.get('content-Type'))) > 0:
            file_type = re.findall(r"application/(.+);", r.headers.get('content-Type'))[0]

    file = Path(save_dir + f"/{file_name}")

    if file_type == "xml":
        with file.open(mode="w") as fp:
            for chunk in r.iter_content(chunk_size=128):
                fp.write(chunk.decode('utf-8'))
    else:
        with file.open(mode="wb") as fp:
            for chunk in r.iter_content(chunk_size=128):
                fp.write(chunk)

    logging.getLogger(LOGGER).debug(f"File saved at {file.absolute()}")
    return file


def open_zip(zip_path: Path, extract_path: str) -> str:
    """
        Open zip and extract content in given path and send first file name in zip
    :param extract_path: Path to where unzip
    :param zip_path: Zip File PAth
    :return: File name in zip.
    """
    logging.getLogger(LOGGER).info(f"Unzipping file ({zip_path.absolute()})")
    import zipfile
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        files = zip_ref.namelist()
        if len(files) == 0:
            raise Exception("Zip file empty")
        files = files[0]
        zip_ref.extractall(extract_path)
    return files


def parse_main_xml(xml_file: Path) -> Union[None, str]:
    """
        Parse the Main XML file. and return the Link with file_type DLTINS
    :param xml_file: XML file path to be parse.
    :return:
    """
    import xmltodict
    with xml_file.open(mode="r") as fp:
        xml_data_dict = xmltodict.parse(fp.read())

    link = None
    for i in xml_data_dict["response"]["result"]['doc']:
        file_type = None
        for j in i["str"]:
            if j["@name"] == "download_link":
                link = j["#text"]
                logging.getLogger(LOGGER).debug(f"download_link tag value is ({link}).")
            if j["@name"] == "file_type":
                file_type = j['#text']
                logging.getLogger(LOGGER).debug(f"file_type tag value is ({file_type}).")
        if file_type == 'DLTINS' and link is not None:
            logging.getLogger(LOGGER).info(f"Found download_link({link}) with file_type({file_type})")
            break
    return link


def parse(xml_file: Path, tags: List[str], csv_collector: DictWriter, max_row: int = None) -> None:
    """
        Parse the Data XML,
    :param xml_file: Path of the XML file to process.
    :param tags: Tags those needs to be collected.
    :param csv_collector: CSV collector Handler.
    :param max_row: Max CSV row to collect, for testing purpose.
    :return:
    """
    logging.getLogger(LOGGER).info(f"Parsing for XML({xml_file.absolute()}) started.")
    import xml.etree.cElementTree as ET
    from copy import deepcopy
    from datetime import datetime

    main_tag = [tag.split(".")[0] if len(tag.split(".")) > 1 else tag for tag in tags]
    logging.getLogger(LOGGER).debug(f"Main tag to be looking ({main_tag})")
    values_template = {tag: "" for tag in tags}

    context = ET.iterparse(xml_file, events=("start", "end"))

    values = deepcopy(values_template)

    parent = None
    __parent = []
    import re
    count = 0
    start_time = datetime.utcnow()
    for event, elem in context:
        tag = re.sub("{.*}", "", elem.tag) if re.match("{.*}", elem.tag) else elem.tag
        text = elem.text

        if event == "start":
            __parent.append(tag)
        elif event == "end":
            __parent.pop(-1)

        if tag in tags and text:
            values[tag] = text
            continue

        if len(__parent) > 1 and f"{__parent[-2]}.{tag}" in tags and text:
            values[f"{__parent[-2]}.{tag}"] = text
            continue

        if event == "end" and len(__parent) > 0 and f"{__parent[-1]}.{tag}" in tags and text:
            values[f"{__parent[-1]}.{tag}"] = text

        if parent is None and tag in main_tag:
            parent = __parent[-1]

        if event == 'end' and tag == parent:
            count += 1
            logging.getLogger(LOGGER).debug(f"Row collected {values}")
            csv_collector.writerow(values)
            values = deepcopy(values_template)
        elem.clear()

        if max_row is not None and isinstance(max_row, int) and count >= max_row:
            logging.getLogger(LOGGER).info(f"Max row({max_row}) hit, breaking.")
            break
    logging.getLogger(LOGGER).info(f"{count} row collected, returning.")
    logging.getLogger(LOGGER).info(f"Data collected in {(datetime.utcnow() - start_time).seconds} seconds.")
    return


def upload_to_aws_s3(csv_path: Path, s3_bucket: str):
    """
        Upload the file to S3 Bucket.
    :param csv_path:
    :param s3_bucket:
    :return:
    """
    from botocore.exceptions import ClientError
    try:
        s3 = boto3.resource(
            service_name='s3'
        )
        if s3_bucket in s3.buckets.all():
            logging.getLogger(LOGGER).info(f"Bucket({s3_bucket}) already exists.")
        else:
            logging.getLogger(LOGGER).info(f"Creating S3 bucket ({s3_bucket})")
            s3.create_bucket(Bucket=s3_bucket)

        logging.getLogger(LOGGER).info(f"Upload file {csv_path.absolute()} to S3 Bucket ({s3_bucket})")
        s3.upload_file(Filename=csv_path, Bucket=s3_bucket, Key=csv_path.name)
        logging.getLogger(LOGGER).info(f"Upload file {csv_path.absolute()} to S3 Bucket ({s3_bucket}) done.")
    except ClientError as exp:
        logging.getLogger(LOGGER).error("Client Error connection to S3 bucket.")
        logging.getLogger(LOGGER).error(exp)
        return False
    return True


def enable_logging_app_factory(log_file: Path, level) -> logging.Logger:
    """
        Enable logging for the system.
    :param level: Logging Level
    :param log_file: Log File path
    :return:
    """
    from logging.handlers import RotatingFileHandler
    import sys

    logger = logging.getLogger(LOGGER)
    formatter = logging.Formatter(LOGGER + ': %(asctime)s %(levelname)7s: %(message)s')

    fileHandler = RotatingFileHandler(log_file, mode="a+", maxBytes=5000000, backupCount=5)
    fileHandler.setFormatter(formatter)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(formatter)
    consoleHandler.setLevel(logging.INFO)

    logger.setLevel(level)
    logger.addHandler(fileHandler)
    logger.addHandler(consoleHandler)

    return logger


def main(xml_link):
    """
        Process :
        Download the xml from this link
        From the xml, please parse through to the first download link whose file_type is DLTINS and download the zip
        Extract the xml from the zip.
        Convert the contents of the xml into a CSV with the following header:
        1. FinInstrmGnlAttrbts.Id
        2. FinInstrmGnlAttrbts.FullNm
        3. FinInstrmGnlAttrbts.ClssfctnTp
        4. FinInstrmGnlAttrbts.CmmdtyDerivInd
        5. FinInstrmGnlAttrbts.NtnlCcy
        6. Issr
        Store the csv from step 4) in an AWS S3 bucket

    :return:
    """
    os.makedirs("tmp", exist_ok=True)
    os.makedirs("log", exist_ok=True)
    enable_logging_app_factory(Path("log/main.log"), logging.DEBUG)
    xml_file = download_file(xml_link, TMP_DIR)
    zip_url = parse_main_xml(xml_file)

    zip_path = download_file(zip_url, TMP_DIR, zip_url.split('/')[-1])
    zip_file = open_zip(zip_path, TMP_DIR)
    tags = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]

    zip_file = Path(TMP_DIR + "/" + zip_file)
    csv_path = Path(TMP_DIR + "/" + zip_file.stem + ".csv")
    with open(csv_path, "w", newline='', encoding="utf-8") as csv_fp:
        csv_collector = DictWriter(csv_fp, delimiter=',', fieldnames=tags)
        csv_collector.writeheader()
        parse(zip_file, tags, csv_collector)

    # Set these paramter.
    os.environ["AWS_DEFAULT_REGION"] = 'us-east-2'
    os.environ["AWS_ACCESS_KEY_ID"] = 'mykey'
    os.environ["AWS_SECRET_ACCESS_KEY"] = 'mysecretkey'

    if upload_to_aws_s3(csv_path, "S3_NEW"):
        logging.getLogger(LOGGER).info("Uploading to AWS to S3 bucket successful.")
    else:
        logging.getLogger(LOGGER).info("Uploading to AWS to S3 bucket failed.")
    return csv_path


if __name__ == '__main__':
    xml_link = """https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date
        :%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"""
    main(xml_link)
