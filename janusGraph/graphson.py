#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export Bitcoin blocks from ClickHouse to GraphSON 3.0 vertices (label: 'block').

- Each line in the output file is a single GraphSON v3 typed vertex (g:Vertex).
- Vertex id and 'bulk_id' use a deterministic string: "block:<block_hash>".
- Properties match your JanusGraph schema: block_hash (String), block_number (Long),
  block_timestamp (Date), previous_block_hash (String, optional).

Why GraphSON v3?
- Works cleanly with TinkerPop readers and BLVP.
- Preserves types (g:String, g:Int64, g:Date), avoiding “mysterious” type coercions.
"""

import os
import json
import argparse
from typing import Dict, Any, Optional

import clickhouse_connect  # pip install clickhouse-connect
from gremlin_python.driver import client, serializer
from clickhouse_connect import get_client

# =============================
# ClickHouse client
# =============================
# === CONFIGURATION ===
#CLICKHOUSE_HOST = '192.168.2.65'
CLICKHOUSE_HOST = 'localhost'

CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'password'
DATABASE = 'bitcoin'
DEFAULT_START_PARTITION = '202506'

END_PARTITION = 201601 # e.g. 201501 for January 2015

# === INIT CLIENT ===
ch = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE,
)

# =============================
# Gremlin client
# =============================
GREMLIN_ENDPOINT = "ws://192.168.2.65:8182/gremlin"

# ↓ drop to something small first; raise later if stable
GREMLIN_REQUEST_TIMEOUT_MS = int(os.getenv("GREMLIN_REQUEST_TIMEOUT_MS", "1200000"))  # 120s

def make_gclient():
    return client.Client(
        GREMLIN_ENDPOINT,
        'g',
        message_serializer=serializer.GraphSONSerializersV3d0()
    )

gclient = make_gclient()

# -------------- Helpers to emit GraphSON v3 typed values --------------

def g_string(v: str) -> Dict[str, Any]:
    return {"@type": "g:String", "@value": v}

def g_int64(v: int) -> Dict[str, Any]:
    return {"@type": "g:Int64", "@value": int(v)}

def g_date_ms(ms: int) -> Dict[str, Any]:
    # Java Date in GraphSON is encoded as g:Date with epoch millis
    return {"@type": "g:Date", "@value": int(ms)}

def vertex_property(prop_id: str, label: str, value_any: Any) -> Dict[str, Any]:
    """
    Build a GraphSON v3 VertexProperty.
    prop_id: deterministic id for the vertex-property (string is fine).
    value_any: either a primitive GraphSON typed dict (g:String, g:Int64, g:Date)
               or a raw primitive (will be wrapped for strings).
    """
    if isinstance(value_any, str):
        v = g_string(value_any)
    elif isinstance(value_any, dict) and "@type" in value_any:
        v = value_any
    else:
        # Fallback: stringify
        v = g_string(str(value_any))

    return {
        "@type": "g:VertexProperty",
        "@value": {
            "id": g_string(prop_id),    # keep simple, string ids are fine
            "label": label,
            "value": v
        }
    }

def make_block_vertex(block_hash: str,
                      block_number: int,
                      ts_ms: int,
                      previous_block_hash: Optional[str]) -> Dict[str, Any]:
    """
    Create a GraphSON v3 vertex for label 'block'.
    id and 'bulk_id' are "block:<block_hash>" for deterministic joins later.
    """
    vid = f"block:{block_hash}"

    props = {
        # mandatory stable id for BLVP-style linking (handy even for vertices)
        "bulk_id": [vertex_property(f"{vid}|bulk_id", "bulk_id", g_string(vid))],
        "block_hash": [vertex_property(f"{vid}|block_hash", "block_hash", g_string(block_hash))],
        "block_number": [vertex_property(f"{vid}|block_number", "block_number", g_int64(block_number))],
        "block_timestamp": [vertex_property(f"{vid}|block_timestamp", "block_timestamp", g_date_ms(ts_ms))],
    }

    if previous_block_hash:
        props["previous_block_hash"] = [
            vertex_property(f"{vid}|previous_block_hash", "previous_block_hash", g_string(previous_block_hash))
        ]

    vertex = {
        "@type": "g:Vertex",
        "@value": {
            "id": g_string(vid),        # GraphSON v3 id (string)
            "label": "block",
            "properties": props
        }
    }
    return vertex

# -------------- ClickHouse → GraphSON exporter --------------

def export_block_vertex(
    client,
    partition: str,
    out_path: str
) -> int:
    """
    Query one block (partition + block_number) and write GraphSON v3 vertex to out_path.
    Returns the number of vertices written (0 or 1).
    """
    print(f"[EXPORT] start: partition={partition} ")

    # Use epoch milliseconds to align with Java Date expectation.
    q = f"""
    SELECT
        hash AS block_hash,
        number AS block_number,
        toUnixTimestamp(timestamp) AS ts_ms,
        previous_block_hash
    FROM blocks
    WHERE toYYYYMM(timestamp) = {partition}
    limit 10
    """

    rs = client.query(q)

    # clickhouse-connect returns rows as tuples by default + column_names
    cols = rs.column_names
    print(f"[EXPORT] got {len(rs.result_rows)} rows")
    if not rs.result_rows:
        print("[EXPORT] no rows found")
        return 0

    # prepare writer
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    written = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for row in rs.result_rows:
            rec = dict(zip(cols, row))
            print(f"[EXPORT] processing block {rec['block_hash']} (number {rec['block_number']})")
            v = make_block_vertex(
                block_hash=rec["block_hash"],
                block_number=int(rec["block_number"]),
                ts_ms=int(rec["ts_ms"]),
                previous_block_hash=rec.get("previous_block_hash") or None
            )
            f.write(json.dumps(v, ensure_ascii=False) + "\n")
            written += 1

    print(f"[EXPORT] done: wrote {written} vertex → {out_path}")
    return written

# -------------- CLI --------------

def main():
    print("Bitcoin blocks export to GraphSON v3")
    print("================================")
    parser = argparse.ArgumentParser(description="Deduplicate partitions in blocks from a start partition onward")
    parser.add_argument("--partition", required=True, help="YYYYMM (toYYYYMM(timestamp))")
    args = parser.parse_args()
    out_path = f"./{args.partition}.graphsonl"

    print(f"Exporting blocks for partition {args.partition} to {out_path}")
    export_block_vertex(ch, args.partition, out_path)



if __name__ == "__main__":
    main()
