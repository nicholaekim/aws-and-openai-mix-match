# AWS PDF to Google Sheets Pipeline

This repository contains a Python script that processes PDF files stored in an S3 bucket using **Amazon Textract** and writes the extracted information to a Google Sheets spreadsheet. It demonstrates how to combine AWS services with Google APIs for document processing workflows.

## Features

- Lists PDF files in a specified S3 bucket and prefix.
- Uses Textract to extract form fields and table data from each document.
- Writes form data and table rows to separate tabs within a Google Sheet.

## Prerequisites

- AWS credentials with permissions for Textract and S3.
- A Google Cloud service account JSON key with access to the target spreadsheet.
- Python 3.8+ with dependencies from `requirements.txt` installed.

## Usage

Set the following environment variables before running the script:

- `S3_BUCKET` – name of the bucket containing PDFs.
- `S3_PREFIX` – optional prefix within the bucket.
- `SPREADSHEET_ID` – ID of the Google Sheet to update.
- `GOOGLE_SHEETS_KEYFILE` – path to the service account JSON key (default `credentials.json`).
- `FORM_TAB` – tab name for form data (default `Form Data`).
- `TABLE_TAB` – tab name for table data (default `Table Data`).

Then run:

```bash
python src/pdf_to_sheet.py
```

The script will process each PDF found in the bucket and append results to the specified Google Sheet tabs.

