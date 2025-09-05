#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ClickHouse → JanusGraph partition sync
- Blocks by toYYYYMM(timestamp)
- Transactions by toYYYYMM(block_timestamp)
- Outputs by toYYYYMM(block_timestamp)
Only writes allowed properties for each label.
Creates edges: belongs_to, coinbase, lock_to, spent_by, pay_to.
"""

import os
import math
import time
from datetime import datetime
from typing import List, Dict, Any, Iterable

import clickhouse_connect
from clickhouse_connect import get_client
from gremlin_python.driver.client import Client as GremlinClient

# =============================
# CONFIG
# =============================
CLICKHOUSE_HOST = os.getenv("CH_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CH_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CH_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CH_PASSWORD", "password")
CLICKHOUSE_DB = os.getenv("CH_DB", "bitcoin")

# Gremlin Server (JanusGraph) WebSocket endpoint
GREMLIN_URL = os.getenv("GREMLIN_URL", "ws://localhost:8182/gremlin")

# Batch sizes (tune to your infra)
READ_LIMIT = int(os.getenv("READ_LIMIT", "5000"))       # fetch from ClickHouse per chunk
GREMLIN_BATCH = int(os.getenv("GREMLIN_BATCH", "1000")) # send to Gremlin per chunk

# =============================
# ClickHouse client
# =============================
ch = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB,
)

# =============================
# Gremlin client
# =============================
gclient = GremlinClient(GREMLIN_URL, "g")


# =============================
# Helpers
# =============================
def chunked(iterable: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def fetch_partitions(table: str, ts_col: str) -> List[str]:
    """
    Returns partitions in YYYYMM (string) for the given table using its timestamp column.
    """
    q = f"""
    SELECT DISTINCT toYYYYMM({ts_col}) AS p
    FROM {table}
    ORDER BY p
    """
    rs = ch.query(q)
    parts = [str(row[0]) for row in rs.result_rows]
    print(f"[INFO] {table}: found {len(parts)} partitions")
    return parts


def fetch_rows_in_chunks(query: str, params: Dict[str, Any] = None, limit: int = READ_LIMIT):
    """
    Yields rows (as dicts) in chunks from ClickHouse using LIMIT/OFFSET pagination.
    """
    offset = 0
    while True:
        q = f"{query} LIMIT {limit} OFFSET {offset}"
        rs = ch.query(q, parameters=params or {})
        rows = rs.dict_result()
        if not rows:
            break
        yield rows
        offset += limit


# =============================
# Gremlin upsert/batch scripts
# =============================

# 1) Upsert Blocks and (optional) chain_to to previous (only if you pass prev fields)
G_SCRIPT_BLOCKS = """
// params: rows = [[block_hash, block_number (Long), block_ts_ms (Long), prev_block_hash?], ...]
for (r in rows) {
  def bh = r[0]; def bn = r[1]; def tms = r[2];
  def prev = (r.size() > 3 ? r[3] : null)
  def vb = g.V().has('block','block_hash', bh).fold().
             coalesce(unfold(),
                      addV('block').property('block_hash', bh)
                                   .property('block_number', bn)
                                   .property('block_timestamp', new Date(tms))
             ).next()
  // update props (idempotent)
  g.V(vb).property('block_number', bn).iterate()
  g.V(vb).property('block_timestamp', new Date(tms)).iterate()

  if (prev != null && prev.length() > 0) {
    def vprev = g.V().has('block','block_hash', prev).fold().
                  coalesce(unfold(),
                           addV('block').property('block_hash', prev)
                  ).next()
    // chain_to: current block -> previous block (many children to one prev)
    g.V(vb).coalesce(outE('chain_to').where(inV().hasId(id(vprev))).limit(1),
                     addE('chain_to').to(vprev)).iterate()
  }
}
"""

# 2) Upsert Transactions and belongs_to & coinbase
G_SCRIPT_TX = """
// params: rows = [[tx_hash, is_coinbase (boolean), block_hash, block_number (Long), block_ts_ms (Long)], ...]
for (r in rows) {
  def th = r[0]; def cb = r[1]; def bh = r[2]; def bn = r[3]; def tms = r[4];

  def vt = g.V().has('transaction','transaction_hash', th).fold().
             coalesce(unfold(),
                      addV('transaction').property('transaction_hash', th)
             ).next()
  // set/refresh properties (idempotent)
  if (cb != null) { g.V(vt).property('is_coinbase', cb).iterate() }

  // upsert block
  def vb = g.V().has('block','block_hash', bh).fold().
             coalesce(unfold(),
                      addV('block').property('block_hash', bh)
                                   .property('block_number', bn)
                                   .property('block_timestamp', new Date(tms))
             ).next()
  // ensure belongs_to tx->block
  g.V(vt).coalesce(outE('belongs_to').where(inV().hasId(id(vb))).limit(1),
                   addE('belongs_to').to(vb)).iterate()

  // coinbase edge block->transaction iff is_coinbase
  if (cb == true) {
    g.V(vb).coalesce(outE('coinbase').where(inV().hasId(id(vt))).limit(1),
                     addE('coinbase').to(vt)).iterate()
  }
}
"""

# 3) Upsert Outputs, lock_to, spent_by, pay_to, coinbase (if row-level is_coinbase)
G_SCRIPT_OUTPUTS = """
/*
rows = [
  [tx_hash, vout (int), otype, ovalue (double),
   block_hash, block_number (Long), block_ts_ms (Long),
   spent_tx_hash (nullable), spent_input_index (nullable int),
   is_coinbase (boolean),
   addresses (List<String>)],
  ...
]
*/
for (r in rows) {
  def th = r[0]; def vout = r[1]; def otype = r[2]; def oval = r[3];
  def bh = r[4]; def bn = r[5]; def tms = r[6];
  def spendTh = r[7]; def spendIdx = r[8];
  def iscb = r[9];
  def addrs = r[10];

  // upsert parent transaction
  def vt = g.V().has('transaction','transaction_hash', th).fold().
             coalesce(unfold(),
                      addV('transaction').property('transaction_hash', th)
             ).next()

  // upsert block (used for coinbase and to ensure consistency)
  def vb = g.V().has('block','block_hash', bh).fold().
             coalesce(unfold(),
                      addV('block').property('block_hash', bh)
                                   .property('block_number', bn)
                                   .property('block_timestamp', new Date(tms))
             ).next()

  // ensure belongs_to (tx->block), idempotent
  g.V(vt).coalesce(outE('belongs_to').where(inV().hasId(id(vb))).limit(1),
                   addE('belongs_to').to(vb)).iterate()

  // upsert output
  def vo = g.V().has('output','transaction_hash', th).has('output_index', vout).fold().
             coalesce(unfold(),
                      addV('output').property('transaction_hash', th)
                                    .property('output_index', vout)
             ).next()
  // set/refresh only allowed properties
  g.V(vo).property('output_type', otype).iterate()
  if (oval != null) g.V(vo).property('output_value', (double)oval).iterate()

  // lock_to (tx -> output)
  g.V(vt).coalesce(outE('lock_to').where(inV().hasId(id(vo))).limit(1),
                   addE('lock_to').to(vo)).iterate()

  // pay_to for each address
  if (addrs != null) {
    for (addr in addrs) {
      def va = g.V().has('address','address', addr).fold().
                 coalesce(unfold(), addV('address').property('address', addr)).next()
      g.V(vo).coalesce(outE('pay_to').where(inV().hasId(id(va))).limit(1),
                       addE('pay_to').to(va)).iterate()
    }
  }

  // spent_by if spendTh provided (one-time spend)
  if (spendTh != null && spendTh.length() > 0) {
    def vsp = g.V().has('transaction','transaction_hash', spendTh).fold().
                coalesce(unfold(),
                         addV('transaction').property('transaction_hash', spendTh)
                ).next()
    g.V(vo).coalesce(outE('spent_by').where(inV().hasId(id(vsp))).limit(1),
                     addE('spent_by').to(vsp)).iterate()
  }

  // coinbase (block -> tx) if is_coinbase true at output-row level
  if (iscb == true) {
    g.V(vb).coalesce(outE('coinbase').where(inV().hasId(id(vt))).limit(1),
                     addE('coinbase').to(vt)).iterate()
  }
}
"""


def gremlin_exec(script: str, bindings: Dict[str, Any]):
    return gclient.submit(script, bindings).all().result()


# =============================
# Blocks sync
# =============================
def sync_blocks_partition(part_yyyymm: str):
    print(f"[BLOCKS] Start partition {part_yyyymm}")
    # Keep only needed columns
    base_q = f"""
    SELECT
      hash AS block_hash,
      number AS block_number,
      toUnixTimestamp64Milli(timestamp) AS ts_ms,
      previous_block_hash
    FROM blocks
    WHERE toYYYYMM(timestamp) = {part_yyyymm}
    ORDER BY timestamp, number
    """
    for rows in fetch_rows_in_chunks(base_q):
        # Build payload for gremlin: [block_hash, block_number, ts_ms, prev_block_hash]
        payload = []
        for r in rows:
            payload.append([
                r.get("block_hash"),
                int(r.get("block_number")) if r.get("block_number") is not None else None,
                int(r.get("ts_ms")) if r.get("ts_ms") is not None else None,
                r.get("previous_block_hash"),
            ])
        for batch in chunked(payload, GREMLIN_BATCH):
            gremlin_exec(G_SCRIPT_BLOCKS, {"rows": batch})
    print(f"[BLOCKS] Done partition {part_yyyymm}")


# =============================
# Transactions sync
# =============================
def sync_transactions_partition(part_yyyymm: str):
    print(f"[TX] Start partition {part_yyyymm}")
    base_q = f"""
    SELECT
      hash AS tx_hash,
      is_coinbase,
      block_hash,
      block_number,
      toUnixTimestamp64Milli(block_timestamp) AS ts_ms
    FROM transactions
    WHERE toYYYYMM(block_timestamp) = {part_yyyymm}
    ORDER BY block_timestamp, tx_hash
    """
    for rows in fetch_rows_in_chunks(base_q):
        payload = []
        for r in rows:
            payload.append([
                r.get("tx_hash"),
                bool(r.get("is_coinbase")) if r.get("is_coinbase") is not None else None,
                r.get("block_hash"),
                int(r.get("block_number")) if r.get("block_number") is not None else None,
                int(r.get("ts_ms")) if r.get("ts_ms") is not None else None,
            ])
        for batch in chunked(payload, GREMLIN_BATCH):
            gremlin_exec(G_SCRIPT_TX, {"rows": batch})
    print(f"[TX] Done partition {part_yyyymm}")


# =============================
# Outputs sync
# =============================
def sync_outputs_partition(part_yyyymm: str):
    print(f"[OUT] Start partition {part_yyyymm}")
    base_q = f"""
    SELECT
      transaction_hash,
      toInt32(output_index) AS output_index,
      type,
      value,
      block_hash,
      block_number,
      toUnixTimestamp64Milli(block_timestamp) AS ts_ms,
      spent_transaction_hash,
      spent_input_index,
      is_coinbase,
      addresses,
      revision
    FROM outputs
    WHERE toYYYYMM(block_timestamp) = {part_yyyymm}
    ORDER BY block_timestamp, transaction_hash, output_index
    """
    for rows in fetch_rows_in_chunks(base_q):
        # filter: spent_by only when revision == 1 (your rule)
        payload = []
        for r in rows:
            spend_th = r.get("spent_transaction_hash")
            if r.get("revision") != 1:
                # if not revision 1, ignore spent info for this row
                spend_th = None
            payload.append([
                r.get("transaction_hash"),
                int(r.get("output_index")) if r.get("output_index") is not None else None,
                r.get("type"),
                float(r.get("value")) if r.get("value") is not None else None,
                r.get("block_hash"),
                int(r.get("block_number")) if r.get("block_number") is not None else None,
                int(r.get("ts_ms")) if r.get("ts_ms") is not None else None,
                spend_th,
                int(r.get("spent_input_index")) if r.get("spent_input_index") is not None else None,
                bool(r.get("is_coinbase")) if r.get("is_coinbase") is not None else False,
                r.get("addresses") or [],
            ])
        for batch in chunked(payload, GREMLIN_BATCH):
            gremlin_exec(G_SCRIPT_OUTPUTS, {"rows": batch})
    print(f"[OUT] Done partition {part_yyyymm}")


# =============================
# Orchestrator
# =============================
def main():
    start = time.time()
    print("[START] Partition-wise sync from ClickHouse to JanusGraph")

    # 1) Blocks
    for p in fetch_partitions("blocks", "timestamp"):
        sync_blocks_partition(p)

    # 2) Transactions
    for p in fetch_partitions("transactions", "block_timestamp"):
        sync_transactions_partition(p)

    # 3) Outputs
    for p in fetch_partitions("outputs", "block_timestamp"):
        sync_outputs_partition(p)

    print(f"[END] Sync finished in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
