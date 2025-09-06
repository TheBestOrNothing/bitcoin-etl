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
import argparse

from clickhouse_connect import get_client
from gremlin_python.driver import client, serializer

# =============================
# CONFIG
# =============================

# Batch sizes (tune to your infra)
READ_LIMIT = int(os.getenv("READ_LIMIT", "5000"))       # fetch from ClickHouse per chunk
GREMLIN_BATCH = int(os.getenv("GREMLIN_BATCH", "1000")) # send to Gremlin per chunk

# =============================
# ClickHouse client
# =============================
# === CONFIGURATION ===
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'password'
DATABASE = 'bitcoin'
DEFAULT_START_PARTITION = '200901'

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
gclient = client.Client(
    GREMLIN_ENDPOINT,
    'g',
    message_serializer=serializer.GraphSONSerializersV3d0()
)

# =============================
# Helpers
# =============================
def get_partitions(client, start_partition, end_partition):
    """
    Return partitions (YYYYMM) for 'blocks' from start to end (inclusive), DESC order.
    If start == end, returns at most one element (only if it exists).
    """

    if not end_partition:
        end_partition = DEFAULT_START_PARTITION
    # normalize & guard
    si = int(start_partition)
    ei = int(end_partition)
    if si > ei:
        si, ei = ei, si  # swap to keep range sane

    if si == ei:
        query = f"""
            SELECT DISTINCT partition
            FROM system.parts
            WHERE database = '{DATABASE}'
              AND table = 'blocks'
              AND active
              AND partition = '{si}'
            ORDER BY toInt64(partition) DESC
        """
    else:
        query = f"""
            SELECT DISTINCT partition
            FROM system.parts
            WHERE database = '{DATABASE}'
              AND table = 'blocks'
              AND active
              AND partition >= '{si}'
              AND partition <= '{ei}'
            ORDER BY toInt64(partition) DESC
        """

    result = client.query(query)
    return [row[0] for row in result.result_rows]


def chunked(iterable: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


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
// rows = [[block_hash(String), block_number(Long), block_ts_ms(Long), prev_block_hash(String?)], ...]
for (r in rows) {
  def bh = r[0]; 
  def bn = r[1] as Long; 
  def tms = r[2] as Long;
  def prev = (r.size() > 3 ? r[3] : null)

  g.V().has('block','block_hash', bh).fold().
    coalesce(
      // If found, throw and stop nothing will be added
      unfold().sideEffect{ throw new IllegalStateException("Duplicate block_hash: " + bh) },

      // If not found, create
      addV('block').
        property('block_hash', bh).
        property('block_number', bn).
        property('block_timestamp', new Date(tms)).
        property('previous_block_hash', prev)
    ).iterate()
}
"""

# 2) Upsert Transactions and belongs_to & coinbase
G_SCRIPT_TX = """
// rows = [[tx_hash, is_coinbase(Boolean), block_hash, block_number(Long), block_ts_ms(Long)], ...]
for (r in rows) {
  def th  = r[0]
  def cb  = r[1]
  def bh  = r[2]
  def bn  = r[3] as Long   // kept for logging if you need it
  def tms = r[4] as Long   // kept for logging if you need it

  // 1) Strict: fail if tx already exists
  def vt = g.V().has('transaction','transaction_hash', th).fold().
    coalesce(
      unfold().sideEffect{ throw new IllegalStateException("Duplicate transaction_hash: " + th) },
      addV('transaction').property('transaction_hash', th)
                         .property('is_coinbase', cb)
    ).next()

  // 2) Strict: require the block to already exist (don’t upsert it here)
  def vb = g.V().has('block','block_hash', bh).fold().
    coalesce(
      unfold(),
      sideEffect{ throw new IllegalStateException("Missing block for tx " + th + " (block_hash=" + bh + ")") }
    ).next()

  // 3) Edges (idempotent creation)
  g.V(vt).coalesce(
    outE('belongs_to').where(inV().hasId(vb.id())).limit(1)
        .sideEffect{ throw new IllegalStateException("belongs_to already exists (tx->block)") },
    addE('belongs_to').to(vb)
  ).iterate()

  if (cb == true) {
    g.V(vb).coalesce(
      outE('coinbase').where(inV().hasId(vt.id())).limit(1)
        .sideEffect{ throw new IllegalStateException("coinbase already exists (block->tx)") },
      addE('coinbase').to(vt)
    ).iterate()
  }
}
"""


# 3) Upsert Outputs, lock_to, spent_by, pay_to, coinbase (if row-level is_coinbase)
G_SCRIPT_OUTPUTS= """
/*
rows = [
  [tx_hash, vout (int), otype, ovalue (double),
   block_hash, block_number (Long), block_ts_ms (Long),
   spent_tx_hash (nullable), spent_input_index (nullable int),
   is_coinbase (boolean),
   addresses (List<String>)],
   revision (int, for filtering spent_by)
  ...
]
*/
for (r in rows) {
  def th      = r[0]
  def vout    = r[1] as Integer
  def otype   = r[2]
  def oval    = r[3] as Double
  def bh      = r[4]
  def bn      = r[5] as Long       // kept for logging/validation if needed
  def tms     = r[6] as Long       // kept for logging/validation if needed
  def spendTh = r[7]
  def spendIx = r[8] as Integer
  def iscb    = r[9]
  def addrs   = r[10]
  def revision     = r[11] as Integer // kept for filtering spent_by if needed

  // 1) STRICT: transaction must exist
  def vt = g.V().has('transaction','transaction_hash', th).fold().
             coalesce(
               unfold(),
               sideEffect{ throw new IllegalStateException("Missing transaction: " + th) }
             ).next()

  // 2) STRICT: block must exist
  def vb = g.V().has('block','block_hash', bh).fold().
             coalesce(
               unfold(),
               sideEffect{ throw new IllegalStateException("Missing block: " + bh) }
             ).next()

  // 3) STRICT: belongs_to (tx -> block) must exist
  g.V(vt).as('t').V(vb).as('b').
    coalesce(
      select('t').outE('belongs_to').where(inV().as('b')).limit(1),
      sideEffect{ throw new IllegalStateException("Missing belongs_to edge tx->block for tx=" + th + ", block=" + bh) }
    ).iterate()

  // 4) STRICT (conditional): coinbase (block -> tx) must exist when is_coinbase == true
  if (iscb == true) {
    g.V(vb).as('b').V(vt).as('t').
      coalesce(
        select('b').outE('coinbase').where(inV().as('t')).limit(1),
        sideEffect{ throw new IllegalStateException("Missing coinbase edge block->tx for tx=" + th + ", block=" + bh) }
      ).iterate()
  }

  // 5) Output: create if missing; then set allowed props
  def vo = g.V().has('output','transaction_hash', th).has('output_index', vout).fold().
             coalesce(
               unfold(),
               addV('output').property('transaction_hash', th).property('output_index', vout)
             ).next()

  if (otype != null) { g.V(vo).property('output_type', otype).iterate() }
  if (oval  != null) { g.V(vo).property('output_value', (double) oval).iterate() }

  // 6) lock_to (tx -> output), idempotent create
  g.V(vt).
    coalesce(
      outE('lock_to').where(inV().hasId(vo.id())).limit(1),
      addE('lock_to').to(vo)
    ).iterate()
  g.V(vt).outE('lock_to').where(inV().hasId(vo.id())).property('output_index', vout).iterate()

  // 7) pay_to (output -> address), idempotent create per address
  if (addrs != null) {
    for (addr in addrs) {
      def va = g.V().has('address','address', addr).fold().
                 coalesce(unfold(), addV('address').property('address', addr)).next()
      g.V(vo).
        coalesce(
          outE('pay_to').where(inV().hasId(va.id())).limit(1),
          addE('pay_to').to(va)
        ).iterate()
    }
  }

  // 8) spent_by (output -> spending tx) if spendTh provided: STRICT require spender tx exist
  if (revision > 0) {
    def vsp = g.V().has('transaction','transaction_hash', spendTh).fold().
                coalesce(
                  unfold(),
                  sideEffect{ throw new IllegalStateException("Missing spender transaction: " + spendTh + " for output " + th + ":" + vout) }
                ).next()
    g.V(vo).
      coalesce(
        outE('spent_by').where(inV().hasId(vsp.id())).limit(1),
        addE('spent_by').to(vsp)
      ).iterate()
    g.V(vo).outE('spent_by').where(inV().hasId(vsp.id())).property('input_index', spendIx).iterate()
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
    ORDER BY number
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
    ORDER BY tx_hash
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
    ORDER BY transaction_hash, output_index
    """
    for rows in fetch_rows_in_chunks(base_q):
        # filter: spent_by only when revision == 1 (your rule)
        payload = []
        for r in rows:
            payload.append([
                r.get("transaction_hash"),
                int(r.get("output_index")) if r.get("output_index") is not None else None,
                r.get("type"),
                float(r.get("value")) if r.get("value") is not None else None,
                r.get("block_hash"),
                int(r.get("block_number")) if r.get("block_number") is not None else None,
                int(r.get("ts_ms")) if r.get("ts_ms") is not None else None,
                r.get("spent_transaction_hash"),
                int(r.get("spent_input_index")) if r.get("spent_input_index") is not None else None,
                bool(r.get("is_coinbase")) if r.get("is_coinbase") is not None else False,
                r.get("addresses") or [],
                r.get("revision"),
            ])
        for batch in chunked(payload, GREMLIN_BATCH):
            gremlin_exec(G_SCRIPT_OUTPUTS, {"rows": batch})
    print(f"[OUT] Done partition {part_yyyymm}")


def main():
    parser = argparse.ArgumentParser(description="Deduplicate partitions in blocks from a start partition onward")
    parser.add_argument("--start-partition", help="Start partition (YYYYMM)")
    parser.add_argument("--end-partition", help="End partition (YYYYMM)")
    args = parser.parse_args()

    partitions = get_partitions(ch, args.start_partition, args.end_partition)
    if not partitions:
        print("No partitions found.")
        return

    for partition in partitions:
        try:
            start_date = datetime.now()
            print(f"Processing partition {partition} at {start_date.strftime('%Y-%m-%d %H:%M:%S')}...")
            start_time = time.perf_counter()

            sync_blocks_partition(partition)
            sync_transactions_partition(partition)
            sync_outputs_partition(partition)   

            end_time = time.perf_counter()
            hours = (end_time - start_time)/3600
            end_date = datetime.now()
            print(f"✅ Finished processing partition {partition} at {end_date.strftime('%Y-%m-%d %H:%M:%S')} and take {hours:.4f} h..")

        except Exception as e:
            print(f"Sync error from clickhouse to janusGraph when processing partition {partition}: {e}")
            return
    

if __name__ == "__main__":
    main()
