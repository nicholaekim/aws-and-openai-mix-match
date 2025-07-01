import os
import time
import json
from typing import List, Dict

import boto3
import gspread
from google.oauth2.service_account import Credentials


AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "")
GOOGLE_SHEETS_KEYFILE = os.getenv("GOOGLE_SHEETS_KEYFILE", "credentials.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
FORM_TAB = os.getenv("FORM_TAB", "Form Data")
TABLE_TAB = os.getenv("TABLE_TAB", "Table Data")


def list_pdfs(s3_client) -> List[str]:
    """List PDF keys under the specified prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".pdf"):
                keys.append(obj["Key"])
    return keys


def analyze_document(textract_client, key: str) -> Dict:
    """Call Textract AnalyzeDocument and return response."""
    response = textract_client.analyze_document(
        Document={"S3Object": {"Bucket": S3_BUCKET, "Name": key}},
        FeatureTypes=["TABLES", "FORMS"],
    )
    return response


def build_kv_map(blocks: List[Dict]) -> Dict[str, str]:
    key_map = {}
    value_map = {}
    block_map = {}
    for block in blocks:
        block_id = block.get("Id")
        block_map[block_id] = block
        if block["BlockType"] == "KEY_VALUE_SET":
            if "KEY" in block.get("EntityTypes", []):
                key_map[block_id] = block
            else:
                value_map[block_id] = block
    kv_pairs = {}
    for key_id, key_block in key_map.items():
        value_block = None
        for rel in key_block.get("Relationships", []):
            if rel["Type"] == "VALUE" and rel.get("Ids"):
                value_id = rel["Ids"][0]
                value_block = value_map.get(value_id)
        key_text = extract_text(key_block, block_map)
        val_text = extract_text(value_block, block_map) if value_block else ""
        kv_pairs[key_text] = val_text
    return kv_pairs


def extract_text(block, block_map) -> str:
    if not block:
        return ""
    text = ""
    for rel in block.get("Relationships", []):
        if rel["Type"] == "CHILD" and rel.get("Ids"):
            for cid in rel["Ids"]:
                word = block_map[cid]
                if word.get("BlockType") == "WORD":
                    text += word.get("Text", "") + " "
                elif word.get("BlockType") == "SELECTION_ELEMENT":
                    if word.get("SelectionStatus") == "SELECTED":
                        text += "X "
    return text.strip()


def extract_tables(blocks: List[Dict]) -> List[List[str]]:
    block_map = {b["Id"]: b for b in blocks}
    tables = []
    for block in blocks:
        if block["BlockType"] == "TABLE":
            rows = {}
            for rel in block.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    for cell_id in rel.get("Ids", []):
                        cell = block_map[cell_id]
                        if cell["BlockType"] == "CELL":
                            row_idx = cell["RowIndex"]
                            col_idx = cell["ColumnIndex"]
                            rows.setdefault(row_idx, {})[col_idx] = extract_text(cell, block_map)
            table_data = []
            for row_idx in sorted(rows.keys()):
                row = rows[row_idx]
                row_values = [row.get(c, "") for c in sorted(row.keys())]
                table_data.append(row_values)
            tables.append(table_data)
    return tables


def authorize_gspread():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(GOOGLE_SHEETS_KEYFILE, scopes=scopes)
    client = gspread.authorize(credentials)
    return client


def append_form_data(sh, data: Dict[str, str]):
    worksheet = sh.worksheet(FORM_TAB)
    headers = worksheet.row_values(1)
    row = [data.get(h, "") for h in headers]
    worksheet.append_row(row)


def append_table_data(sh, table: List[List[str]], source_key: str):
    worksheet = sh.worksheet(TABLE_TAB)
    for row in table:
        worksheet.append_row([source_key] + row)


def process_pdf(textract_client, key: str, sh):
    response = analyze_document(textract_client, key)
    blocks = response.get("Blocks", [])
    kv_pairs = build_kv_map(blocks)
    append_form_data(sh, kv_pairs)
    tables = extract_tables(blocks)
    for table in tables:
        append_table_data(sh, table, key)


def main():
    if not all([S3_BUCKET, SPREADSHEET_ID]):
        raise ValueError("S3_BUCKET and SPREADSHEET_ID environment variables must be set")

    s3 = boto3.client("s3", region_name=AWS_REGION)
    textract = boto3.client("textract", region_name=AWS_REGION)
    keys = list_pdfs(s3)

    gs_client = authorize_gspread()
    sh = gs_client.open_by_key(SPREADSHEET_ID)

    for key in keys:
        print(f"Processing {key}...", flush=True)
        process_pdf(textract, key, sh)
        time.sleep(0.2)  # avoid hitting API rate limits
    print("Done")


if __name__ == "__main__":
    main()
