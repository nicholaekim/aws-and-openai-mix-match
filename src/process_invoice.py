#!/usr/bin/env python3
"""Process an invoice PDF from S3 using Textract queries and summarize with OpenAI."""

import os
import sys
import logging
from typing import Dict, List

import boto3
import openai
import gspread
from google.oauth2.service_account import Credentials
from dateutil import parser as date_parser

# Constants for Textract adapter (replace with real IDs)
ADAPTER_ID = "YOUR_ADAPTER_ID"
ADAPTER_VERSION = "1"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def load_config() -> Dict[str, str]:
    """Load required configuration from environment variables."""
    env_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "S3_BUCKET_NAME",
        "OPENAI_API_KEY",
        "GOOGLE_SHEET_ID",
        "GOOGLE_CREDENTIALS_FILE",
    ]
    config = {}
    missing = []
    for var in env_vars:
        val = os.getenv(var)
        if not val:
            missing.append(var)
        else:
            config[var] = val
    if missing:
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")
    return config

def analyze_document(textract_client, bucket: str, key: str) -> Dict:
    """Call Textract AnalyzeDocument with queries."""
    try:
        response = textract_client.analyze_document(
            Document={"S3Object": {"Bucket": bucket, "Name": key}},
            FeatureTypes=["QUERIES"],
            QueriesConfig={
                "Queries": [
                    {"Text": "Title", "Alias": "Title"},
                    {"Text": "Date", "Alias": "Date"},
                    {"Text": "Volume/Issue Number", "Alias": "VolumeIssueNumber"},
                ]
            },
            AdaptersConfig={
                "Adapters": [
                    {
                        "AdapterId": ADAPTER_ID,
                        "AdapterVersion": ADAPTER_VERSION,
                    }
                ]
            },
        )
        return response
    except Exception as e:
        logging.error("Textract call failed: %s", e)
        raise

def parse_response(response: Dict) -> Dict[str, str]:
    """Extract query results and full text from Textract response."""
    results: Dict[str, str] = {}
    full_lines: List[str] = []
    for block in response.get("Blocks", []):
        b_type = block.get("BlockType")
        if b_type == "QUERY_RESULT":
            alias = block.get("Query", {}).get("Alias")
            text = block.get("Text", "")
            if alias:
                results[alias] = text
        elif b_type == "LINE":
            full_lines.append(block.get("Text", ""))

    if "Date" in results:
        try:
            dt = date_parser.parse(results["Date"], fuzzy=True)
            results["Date"] = dt.strftime("%Y/%m/%d")
        except Exception as e:
            logging.warning("Failed to parse date '%s': %s", results["Date"], e)

    results["full_text"] = "\n".join(full_lines)
    return results

def summarize_text(full_text: str, api_key: str) -> str:
    """Use OpenAI ChatCompletion to summarize text."""
    openai.api_key = api_key
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a concise summarizer."},
                {"role": "user", "content": full_text},
            ],
            max_tokens=150,
        )
        summary = response["choices"][0]["message"]["content"].strip()
        return summary
    except Exception as e:
        logging.error("OpenAI API call failed: %s", e)
        raise

def append_to_sheet(creds_file: str, sheet_id: str, row: List[str]) -> None:
    """Append a row to the Form Data sheet."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(creds_file, scopes=scopes)
    client = gspread.authorize(credentials)
    try:
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet("Form Data")
        worksheet.append_row(row)
    except Exception as e:
        logging.error("Failed to update Google Sheet: %s", e)
        raise

def main():
    if len(sys.argv) != 2:
        print("Usage: process_invoice.py <s3_key>")
        sys.exit(1)
    s3_key = sys.argv[1]

    config = load_config()
    openai.api_key = config["OPENAI_API_KEY"]

    textract = boto3.client(
        "textract",
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
        region_name=config["AWS_REGION"],
    )

    logging.info("Analyzing %s from bucket %s", s3_key, config["S3_BUCKET_NAME"])
    response = analyze_document(textract, config["S3_BUCKET_NAME"], s3_key)
    parsed = parse_response(response)

    summary = summarize_text(parsed["full_text"], config["OPENAI_API_KEY"])
    parsed["Description"] = summary

    row = [
        parsed.get("Title", ""),
        parsed.get("Date", ""),
        parsed.get("Description", ""),
        parsed.get("VolumeIssueNumber", ""),
    ]
    append_to_sheet(config["GOOGLE_CREDENTIALS_FILE"], config["GOOGLE_SHEET_ID"], row)
    logging.info("Row appended successfully")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("Processing failed: %s", e)
        sys.exit(1)

