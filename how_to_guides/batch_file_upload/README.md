# Batch file upload to Bigdata

Upload multiple files to Bigdata using the REST API. The script reads a list of file paths, uploads each file (POST → PUT to presigned URL → poll until processing completes), and writes results to a CSV.

## Setup

1. **Create a virtual environment (recommended)**

   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Linux/macOS
   # or:  .venv\Scripts\activate   # Windows
   ```

2. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**

   Copy `.env` to a new file if needed, then edit `.env` and set your Bigdata API key:

   ```bash
   cp .env .env.local
   # Edit .env or .env.local and set BIGDATA_API_KEY=your-api-key
   ```

   The script loads variables from `.env` in this directory. You can also set `BIGDATA_API_KEY` (and optionally `BIGDATA_API_BASE_URL`) in your shell.

## Usage

Run the script with these parameters:

- **workdir** – Directory that contains your files and where the log and result CSV will be written.
- **upload_txt_filename** – Name of a text file inside `workdir` that lists files to upload (one path per line; paths are relative to `workdir` unless absolute).
- **max_concurrency** – Number of files to upload in parallel (e.g. `5`).

Create `file_list.txt` in `workdir` with one filename per line, for example:

```
report.pdf
data/other_doc.PDF
```

### Example: run the script

From the `batch_file_upload` directory, with `BIGDATA_API_KEY` set in `.env`:

```bash
cd /path/to/bigdata-api-resources/how_to_guides/batch_file_upload
pip install -r requirements.txt
# Edit .env and set BIGDATA_API_KEY=your-api-key

# Run (paths are relative to your workdir)
python batch_file_upload.py \
  workdir=/home/you/Documents/PDFsamples \
  upload_txt_filename=file_list.txt \
  max_concurrency=5
```

Or set the API key in the shell and run from any directory:

```bash
export BIGDATA_API_KEY=your-api-key
python /path/to/batch_file_upload/batch_file_upload.py \
  workdir=/home/you/Documents/PDFsamples \
  upload_txt_filename=file_list.txt \
  max_concurrency=5
```

The script will:

1. Write a log file in `workdir` (e.g. `bigdata_processing_20260312_120000.log`).
2. Write a result CSV in `workdir` (e.g. `uploaded_file_ids_20260312_120000.csv`) with columns: `file_id`, `upload_status`, `file_path`.

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BIGDATA_API_KEY` | Yes | — | Your Bigdata API key. |
| `BIGDATA_API_BASE_URL` | No | `https://api.bigdata.com` | API base URL. |
| `BIGDATA_RATE_LIMIT_PER_MINUTE` | No | `500` | Max requests per minute (should match your WAF). |
| `BIGDATA_RATE_LIMIT_SAFETY_MARGIN` | No | `20` | Margin under the limit (actual cap = limit − margin). |
| `BIGDATA_POLL_INTERVAL_SEC` | No | `10` | Seconds between status polls while waiting for completion. |
| `BIGDATA_UPLOAD_MAX_RETRIES` | No | `5` | Max retries per file on 429/5xx. |

Variables are loaded from `.env` in this folder; you can override them in the shell.
