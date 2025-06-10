import concurrent
import csv
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from timeit import default_timer as timer

from bigdata_client import Bigdata
from bigdata_client.exceptions import BigdataClientRateLimitError

UPLOAD_DONE = "UPLOAD_DONE"
UPLOAD_ERROR = "UPLOAD_ERROR"

DOWNLOAD_DONE = "DOWNLOAD_DONE"
DOWNLOAD_ERROR = "DOWNLOAD_ERROR"

DELETE_DONE = "DELETE_DONE"
DELETE_ERROR = "DELETE_ERROR"


def configure_logging(workdir: str):
    log_file_path = os.path.join(
        workdir, f"bigdata_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler(),  # This will also print logs to the Jupyter notebook output
        ],
    )


# > UPLOADING FILES -------------------------------
def upload_file(bigdata: Bigdata, file_path: str):
    max_try = 5
    attempt = 0
    while attempt < max_try:
        try:
            # file upload logic
            file = bigdata.uploads.upload_from_disk(file_path, skip_metadata=True)
            return file_path, file.id, UPLOAD_DONE
        except BigdataClientRateLimitError:
            attempt += 1
            # sleep 2, 4 or 8 seconds before next attempt
            time.sleep(min(2**attempt, 10))
            logging.warning(f"{attempt} failed: {file_path}")

    return file_path, "", UPLOAD_ERROR


def bulk_upload_files(
    bigdata: Bigdata,
    max_concurrency: int,
    upload_txt_filename: str,
    result_csv_filename: str,
):
    with (
        open(upload_txt_filename, "r") as upload_txt,
        open(result_csv_filename, "w+", newline="") as result_csv,
        ThreadPoolExecutor(max_workers=max_concurrency) as executor,
    ):
        writer = csv.writer(result_csv)

        feature_to_file = {}
        for file_path in upload_txt:
            file_path = file_path.strip()
            feature_to_file[executor.submit(upload_file, bigdata, file_path)] = (
                file_path
            )

        for future in concurrent.futures.as_completed(fs=feature_to_file):
            file_id = ""
            file_path = feature_to_file[future]
            upload_status = ""
            try:
                file_path, file_id, upload_status = future.result()
                if upload_status == UPLOAD_ERROR:
                    logging.error(
                        f"Error to upload file {file_path}, max retries reached"
                    )
                else:
                    logging.info(f"Success upload file {file_path}")
            except Exception as e:
                upload_status = UPLOAD_ERROR
                logging.error(f"Error to upload file {file_path}", exc_info=e)
            finally:
                row = [file_id, upload_status, file_path]
                writer.writerow(row)


# < END UPLOADING FILES -------------------------------

if __name__ == "__main__":
    params = {param.split("=")[0]: param.split("=")[1] for param in sys.argv[1:]}
    workdir = params["workdir"]
    upload_txt_filename = params["upload_txt_filename"]
    upload_result_csv_filename = (
        f"uploaded_file_ids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    max_concurrency = params["max_concurrency"]

    configure_logging(workdir)
    concurrency = int(max_concurrency)

    bigdata = Bigdata()  # Credentials

    upload_txt_path = os.path.join(workdir, upload_txt_filename)
    upload_result_csv_path = os.path.join(workdir, upload_result_csv_filename)
    upload_start = timer()
    bulk_upload_files(
        bigdata=bigdata,
        max_concurrency=concurrency,
        upload_txt_filename=upload_txt_path,
        result_csv_filename=upload_result_csv_path,
    )
    upload_end = timer()
    total_upload_time = upload_end - upload_start
    logging.info(f"UPLOAD TIME: {total_upload_time}")
