import sqlite3
from sentence_transformers import SentenceTransformer
from chromadb import Client
from chromadb.config import Settings

DB_FILE = "invoices.db"
CHROMA_DB_DIR = "./chroma_db"

def fetch_invoices():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT invoice_id, project_id, vendor_name, contract_name, status, balance_to_finish_including_retainage FROM invoices")
    rows = cur.fetchall()
    conn.close()
    return rows

def create_text_representation(row):
    # row: (invoice_id, project_id, vendor_name, contract_name, status, balance)
    invoice_id, project_id, vendor_name, contract_name, status, balance = row
    text = (
        f"Invoice ID: {invoice_id}, "
        f"Project ID: {project_id}, "
        f"Vendor: {vendor_name}, "
        f"Contract: {contract_name}, "
        f"Status: {status}, "
        f"Balance: {balance}"
    )
    return text

if __name__ == "__main__":
    # Fetch data
    invoices = fetch_invoices()

    # Initialize embedding model (for instance, 'all-MiniLM-L6-v2' is a popular model)
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Initialize Chroma vector DB client
    client = Client(Settings(chroma_db_impl="duckdb+parquet", persist_directory=CHROMA_DB_DIR))

    # Create a collection for invoices (if it doesn’t exist)
    # The collection name will remain consistent throughout the project.
    collection = client.get_or_create_collection(name="invoices_collection")

    # Prepare data for embeddings
    texts = []
    metadatas = []
    ids = []
    for inv in invoices:
        text = create_text_representation(inv)
        texts.append(text)
        metadata = {
            "invoice_id": inv[0],
            "project_id": inv[1],
            "vendor_name": inv[2],
            "contract_name": inv[3],
            "status": inv[4],
            "balance": inv[5]
        }
        metadatas.append(metadata)
        ids.append(str(inv[0]))  # IDs must be strings

    # Compute embeddings
    embeddings = model.encode(texts).tolist()

    # Add to vector store
    collection.add(documents=texts, embeddings=embeddings, metadatas=metadatas, ids=ids)

    print("Embedding and vector store setup completed successfully.")
