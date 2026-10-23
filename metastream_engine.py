import os
import json
import hashlib
import duckdb
import requests
from datetime import datetime, timezone

DB_FILE = "data/metastream.db"
EXPORT_DIR = "public"
API_DIR = os.path.join(EXPORT_DIR, "v1")

def compute_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()

def fetch_pypi_data():
    """Phase 1: Vertical Slice - Fetch PyPI Data"""
    res = requests.get("https://pypi.org/pypi/requests/json", timeout=10)
    return res.json() if res.status_code == 200 else None

def fetch_as_dicts(cursor):
    """Converts a DuckDB cursor directly into a list of dicts without pandas/numpy."""
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def build_static_api(conn):
    """Pre-computes REST API endpoints as static JSON for GitHub Pages"""
    os.makedirs(os.path.join(API_DIR, "entities"), exist_ok=True)
    
    # 1. Export /v1/changes.json
    cur = conn.execute("SELECT * FROM changes ORDER BY change_id DESC LIMIT 100")
    changes = fetch_as_dicts(cur)
    with open(os.path.join(API_DIR, "changes.json"), "w", encoding="utf-8") as f:
        json.dump({"changes": [
            {k: str(v) if k == 'timestamp_utc' else v for k, v in c.items()} 
            for c in changes
        ]}, f, indent=2)

    # 2. Export /v1/entities/{id}.json
    entities = conn.execute("SELECT entity_id FROM entities").fetchall()
    for (ent_id,) in entities:
        cur = conn.execute(f"SELECT * FROM claims WHERE entity_id = '{ent_id}'")
        claims = fetch_as_dicts(cur)
        with open(os.path.join(API_DIR, "entities", f"{ent_id}.json"), "w", encoding="utf-8") as f:
            json.dump({
                "entity_id": ent_id,
                "claims": [{k: str(v) if 'time' in k or 'from' in k else v for k, v in c.items()} for c in claims]
            }, f, indent=2)

def run():
    os.makedirs("data", exist_ok=True)
    os.makedirs(EXPORT_DIR, exist_ok=True)
    
    conn = duckdb.connect(DB_FILE)
    with open("schema.sql", "r", encoding="utf-8") as f:
        conn.execute(f.read())

    now = datetime.now(timezone.utc)
    
    # Ingest Data
    pypi_data = fetch_pypi_data()
    if pypi_data:
        artifact_hash = compute_hash(pypi_data)
        evidence_id = f"evid_{artifact_hash[:16]}"
        entity_id = "ent_pypi_requests"
        version = pypi_data["info"]["version"]
        
        claim_payload = {"version": version}
        claim_hash = compute_hash(claim_payload)
        claim_id = f"clm_{claim_hash[:16]}"

        # Check idempotency
        exists = conn.execute(f"SELECT 1 FROM claims WHERE content_hash = '{claim_hash}'").fetchone()
        
        if not exists:
            # Atomic Database Transaction
            conn.execute("BEGIN TRANSACTION")
            conn.execute(f"INSERT OR IGNORE INTO entities VALUES ('{entity_id}', 'software_package', 'requests')")
            conn.execute(f"INSERT INTO evidence VALUES ('{evidence_id}', 'src_pypi', '{now.isoformat()}', '{artifact_hash}', 'https://github.com/...')")
            conn.execute(f"""
                INSERT INTO claims (claim_id, entity_id, predicate, object_value, source_id, evidence_id, valid_from, first_observed_at, content_hash)
                VALUES ('{claim_id}', '{entity_id}', 'HAS_VERSION', '{json.dumps(claim_payload)}', 'src_pypi', '{evidence_id}', '{now.isoformat()}', '{now.isoformat()}', '{claim_hash}')
            """)
            commit_id = conn.execute("SELECT nextval('seq_commit_id')").fetchone()[0]
            conn.execute(f"INSERT INTO changes (commit_id, entity_id, operation, claim_id, timestamp_utc) VALUES ({commit_id}, '{entity_id}', 'CLAIM_ADDED', '{claim_id}', '{now.isoformat()}')")
            conn.execute("COMMIT")
            print(f"Committed new claim: {claim_id}")

    # Export Parquet Database for AI Agents
    conn.execute(f"EXPORT DATABASE '{EXPORT_DIR}' (FORMAT PARQUET)")
    
    # Export Static REST API
    build_static_api(conn)
    conn.close()

if __name__ == "__main__":
    run()
