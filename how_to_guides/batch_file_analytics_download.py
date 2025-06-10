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


# > DOWNLOAD ANALYTICS FILES -------------------------------
def download_analytics_file(
    bigdata: Bigdata,
    download_timeout: int,
    file_id: str,
    output_directory: str,
    absolute_path_filename: str,
):
    max_try = 5
    attempt = 0
    while attempt < max_try:
        try:
            logging.info(f"downloading file {file_id}")
            filename = os.path.basename(
                absolute_path_filename
            )  # extract filename only from full path
            filename = filename.replace(".", "_")
            filename = os.path.join(output_directory, f"{filename}_analytics.json")

            file = bigdata.uploads.get(file_id)
            logging.info(f"downloading file status {file.status}")
            file.wait_for_analysis_complete(timeout=download_timeout)
            file.download_analytics(filename)

            return file_id, DOWNLOAD_DONE
        except Exception as _:
            attempt += 1
            # sleep 2, 4 or 8 seconds before next attempt
            time.sleep(min(2**attempt, 10))
            logging.warning(f"failed {attempt}: {file_id}")

    return file_id, DOWNLOAD_ERROR


def bulk_download_analytics(
    bigdata: Bigdata,
    max_concurrency: int,
    download_timeout: int,
    output_directory: str,
    csv_filename: str,
    result_csv_filename: str,
):
    with (
        open(csv_filename, "r", newline="") as csv_file,
        open(result_csv_filename, "w+", newline="") as result_csv,
        ThreadPoolExecutor(max_workers=max_concurrency) as executor,
    ):
        reader = csv.reader(csv_file)
        writer = csv.writer(result_csv)
        future_to_file_id = {}
        for row in reader:
            file_id = row[0]
            status = row[1]
            absolute_path_filename = row[2]
            if status == UPLOAD_DONE or status == DOWNLOAD_ERROR:
                future_to_file_id[
                    executor.submit(
                        download_analytics_file,
                        bigdata,
                        download_timeout,
                        file_id,
                        output_directory,
                        absolute_path_filename,
                    )
                ] = (file_id, absolute_path_filename)
        for future in concurrent.futures.as_completed(fs=future_to_file_id):
            file_id, filename = future_to_file_id[future]
            download_status = ""
            try:
                file_id, download_status = future.result()
                if download_status == DOWNLOAD_ERROR:
                    logging.error(
                        f"failed to download file {file_id}, max retries reached"
                    )
                else:
                    logging.info(f"downloaded file {file_id}")
            except Exception as ex:
                download_status = DOWNLOAD_ERROR
                logging.error(f"failed to download file {file_id}", exc_info=ex)
            finally:
                row = [file_id, download_status, filename]
                print(row)
                writer.writerow(row)


# < END DOWNLOAD ANALYTICS FILES -------------------------------

if __name__ == "__main__":
    params = {param.split("=")[0]: param.split("=")[1] for param in sys.argv[1:]}
    workdir = params["workdir"]
    output_dir = params["output_dir"]
    upload_result_csv_filename = params["uploaded_file_ids_csv_filename"]
    download_result_csv_filename = (
        f"download_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    max_concurrency = params["max_concurrency"]
    max_download_timeout = params["max_download_timeout"]
    print(params)

    configure_logging(workdir)
    concurrency = int(max_concurrency)
    download_timeout = int(max_download_timeout)

    upload_result_csv_path = os.path.join(workdir, upload_result_csv_filename)

    bigdata = Bigdata()

    download_start = timer()
    download_result_csv_path = os.path.join(workdir, download_result_csv_filename)
    bulk_download_analytics(
        bigdata,
        max_concurrency=concurrency,
        download_timeout=download_timeout,
        output_directory=output_dir,
        csv_filename=upload_result_csv_path,
        result_csv_filename=download_result_csv_path,
    )
    download_end = timer()
    download_total_time = download_end - download_start
    logging.info(f"DOWNLOAD TIME: {download_total_time}")
