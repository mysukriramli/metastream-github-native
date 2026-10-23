import duckdb
import sys
import json

# Agent downloads the highly-compressed Parquet database from your GitHub Pages
# This satisfies the bitemporal querying requirement without needing a live cloud server.
DB_URL = "https://mysukriramli.github.io/metastream-github-native"

def query_bitemporal(entity_id: str, known_at: str):
    conn = duckdb.connect(":memory:")
    # Query remote Parquet files directly over HTTP
    query = f"""
        SELECT * FROM read_parquet('{DB_URL}/claims.parquet')
        WHERE entity_id = '{entity_id}'
        AND first_observed_at <= '{known_at}'
    """
    result = conn.execute(query).fetchdf().to_dict(orient="records")
    return [ {k: str(v) for k,v in r.items()} for r in result ]

# Standard MCP stdio interface
for line in sys.stdin:
    req = json.loads(line)
    if req.get("method") == "tools/call" and req["params"]["name"] == "get_claims_bitemporal":
        args = req["params"]["arguments"]
        data = query_bitemporal(args["entity_id"], args["known_at"])
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": {"content": [{"type": "text", "text": json.dumps(data)}]}}) + "\n")
        sys.stdout.flush()
