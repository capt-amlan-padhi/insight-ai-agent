import json
import sqlite3
import os

DB_FILE = "invoices.db"
DATA_FILE = "Procore_Subcontractor_Invoices_New.json"



def create_tables(conn):
    with conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id INTEGER PRIMARY KEY,
            project_id INTEGER,
            vendor_name TEXT,
            invoice_number TEXT,
            billing_date TEXT,
            total_claimed_amount REAL,
            contract_name TEXT,
            status TEXT,
            erp_status TEXT,
            balance_to_finish_including_retainage REAL,
            total_earned_less_retainage REAL,
            original_contract_sum REAL,
            contract_sum_to_date REAL
        );
        """)


def insert_invoice(conn, invoice_data):
    invoice_id = invoice_data.get("id")
    project_id = invoice_data.get("project_id")
    vendor_name = invoice_data.get("vendor_name")
    invoice_number = invoice_data.get("invoice_number")
    billing_date = invoice_data.get("billing_date")
    total_claimed_amount = invoice_data.get("total_claimed_amount")
    contract_name = invoice_data.get("contract_name")
    status = invoice_data.get("status")
    erp_status = invoice_data.get("erp_status")

    summary = invoice_data.get("summary", {})
    balance = summary.get("balance_to_finish_including_retainage")
    earned_less_retainage = summary.get("total_earned_less_retainage")
    original_contract_sum = summary.get("original_contract_sum")
    contract_sum_to_date = summary.get("contract_sum_to_date")

    with conn:
        conn.execute("""
            INSERT OR IGNORE INTO invoices (
                invoice_id, project_id, vendor_name, invoice_number, billing_date, 
                total_claimed_amount, contract_name, status, erp_status,
                balance_to_finish_including_retainage, total_earned_less_retainage,
                original_contract_sum, contract_sum_to_date
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?
            );
        """, (
            invoice_id, project_id, vendor_name, invoice_number, billing_date,
            total_claimed_amount, contract_name, status, erp_status,
            balance, earned_less_retainage, original_contract_sum, contract_sum_to_date
        ))


if __name__ == "__main__":
    # Remove existing DB if you want a fresh start (optional)
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    conn = sqlite3.connect(DB_FILE)
    create_tables(conn)

    # Load JSON data
    with open(DATA_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)

    # Insert invoices
    for invoice in data:
        insert_invoice(conn, invoice)

    conn.close()
    print("Data ingestion to SQLite completed.")
