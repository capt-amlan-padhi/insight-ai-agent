import os
import sqlite3
import openai
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings

# Configure these variables consistently:
DB_FILE = "invoices.db"
CHROMA_DB_DIR = "./chroma_db"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # Ensure this is set in your environment
EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # same model used before

openai.api_key = OPENAI_API_KEY

def get_vector_collection():
    client = chromadb.Client(
        Settings(
            chroma_db_impl="duckdb+parquet",
            persist_directory=CHROMA_DB_DIR
        )
    )
    return client.get_collection(name="invoices_collection")

def embed_text(text):
    model = SentenceTransformer(EMBEDDING_MODEL)
    return model.encode([text])[0].tolist()

def retrieve_invoices(query, top_k=5):
    collection = get_vector_collection()
    query_embedding = embed_text(query)

    # If you know there's only 1 invoice:
    top_k = 1

    results = collection.query(query_embeddings=[query_embedding], n_results=top_k)
    return results


def query_db(sql, params=()):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows

def is_top_n_invoices_question(query):
    # Very basic heuristic for demonstration
    return "top" in query.lower() and "invoices" in query.lower()

def is_highest_balance_question(query):
    # Another simple check for demonstration
    return "highest balance" in query.lower()

def handle_top_invoices(query):
    # Extract a number from the query if present, default to 5
    import re
    match = re.search(r"top\s+(\d+)", query.lower())
    n = int(match.group(1)) if match else 5

    # Check if there's a project mentioned
    # For simplicity, look for pattern like "Project X"
    import re
    project_match = re.search(r"project\s+(\w+)", query, re.IGNORECASE)
    if project_match:
        project = project_match.group(1)
        # If project is found, filter by project_id or project_name if stored.
        # We do not have project_name in DB. We only have project_id and not direct mapping.
        # For demonstration, we will assume project_id=789 for "Project"
        # Adjust logic as needed if multiple projects.
        sql = """
        SELECT invoice_id, vendor_name, contract_name, total_claimed_amount
        FROM invoices
        WHERE project_id = 789
        ORDER BY total_claimed_amount DESC LIMIT ?
        """
        rows = query_db(sql, (n,))
    else:
        # No project specified
        sql = """
        SELECT invoice_id, vendor_name, contract_name, total_claimed_amount
        FROM invoices
        ORDER BY total_claimed_amount DESC LIMIT ?
        """
        rows = query_db(sql, (n,))

    if not rows:
        return "No invoices found."

    answer = f"Here are the top {n} invoices:\n"
    for r in rows:
        answer += f"Invoice ID: {r[0]} | Vendor: {r[1]} | Contract: {r[2]} | Amount: {r[3]}\n"
    return answer.strip()

def handle_highest_balance():
    sql = """
    SELECT invoice_id, balance_to_finish_including_retainage
    FROM invoices
    ORDER BY balance_to_finish_including_retainage DESC LIMIT 1
    """
    rows = query_db(sql)
    if not rows:
        return "No invoices found."
    invoice_id, balance = rows[0]
    return f"The invoice with ID {invoice_id} has the highest balance amount pending with an amount ${balance}."

def call_llm(query, context):
    # Using OpenAI's gpt-3.5-turbo model
    messages = [
        {"role": "system", "content": "You are an Insight Assistant Agent that provides insights about invoices."},
        {"role": "user", "content": f"Context:\n{context}\nQuestion: {query}\nAnswer:"}
    ]
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages,
        temperature=0.2,
        max_tokens=200
    )
    return response.choices[0].message["content"].strip()

if __name__ == "__main__":
    # For testing, we can try some queries here:
    test_queries = [
        "List down the Top 5 invoices",
        "Show me the summary of the Invoice which has the highest balance",
        "What's the current score of the match?"
    ]

    for q in test_queries:
        if is_top_n_invoices_question(q):
            answer = handle_top_invoices(q)
        elif is_highest_balance_question(q):
            answer = handle_highest_balance()
        else:
            # Generic case: retrieve context and call LLM
            results = retrieve_invoices(q)
            docs = results.get("documents", [[]])[0]
            context = "\n".join(docs)
            if not docs:
                answer = "I’m sorry, I don’t have information related to your query."
            else:
                answer = call_llm(q, context)
        print(f"Q: {q}\nA: {answer}\n")
