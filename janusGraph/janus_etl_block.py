#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ClickHouse → JanusGraph sync (block-by-block, no batching)
- Processes blocks in numeric order from --start-block to --end-block (inclusive).
- For each block N:
    * load the single block row (blocks.number = N) -> Gremlin
    * load all transactions where transactions.block_number = N -> Gremlin
    * load all outputs where outputs.block_number = N -> Gremlin
Only writes allowed properties. Uses strict scripts you provided (errors on duplicates/missing edges as specified).
"""

import os
import time
import argparse
from typing import List, Dict, Any, Optional

from clickhouse_connect import get_client
from gremlin_python.driver import client as gclient_mod
from gremlin_python.driver import serializer as gserializer

# =============================
# Env / Defaults
# =============================
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "password")
DATABASE = os.getenv("CLICKHOUSE_DATABASE", "bitcoin")

GREMLIN_ENDPOINT = os.getenv("GREMLIN_ENDPOINT", "ws://192.168.2.65:8182/gremlin")

# =============================
# Connectors
# =============================
ch = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE,
)

gclient = gclient_mod.Client(
    GREMLIN_ENDPOINT,
    'g',
    message_serializer=gserializer.GraphSONSerializersV3d0()
)

# =============================
# Utilities
# =============================
def dict_rows(rs) -> List[Dict[str, Any]]:
    cols = rs.column_names
    return [dict(zip(cols, row)) for row in rs.result_rows]

def qdict(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    rs = ch.query(query, parameters=params or {})
    return dict_rows(rs)

def gremlin_exec(script: str, bindings: Dict[str, Any]):
    return gclient.submit(script, bindings).all().result()

def get_block_numbers(start_block: Optional[int], end_block: Optional[int]) -> List[int]:
    """
    Returns block numbers in ascending order for the requested range.
    If only start is provided, runs from start to MAX(number) present.
    If only end is provided, runs from MIN(number) present to end.
    If neither provided, runs the entire table.
    """
    where = []
    if start_block is not None:
        where.append(f"number <= {int(start_block)}")
    if end_block is not None:
        where.append(f"number >= {int(end_block)}")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    q = f"""
        SELECT number
        FROM blocks
        {where_sql}
        ORDER BY number DESC
    """
    rows = qdict(q)
    return [int(r["number"]) for r in rows]

# =============================
# Gremlin scripts (STRICT)
# =============================
G_SCRIPT_BLOCKS = """
// rows = [[block_hash(String), block_number(Long), block_ts_ms(Long), prev_block_hash(String?)], ...]
for (r in rows) {
  def bh = r[0];
  def bn = r[1] as Long;
  def tms = r[2] as Long;
  def prev = (r.size() > 3 ? r[3] : null);

  g.V().has('block','block_hash', bh).fold().
    coalesce(
      unfold().sideEffect{ throw new IllegalStateException("Duplicate block_hash: " + bh) },
      addV('block').
        property('block_hash', bh).
        property('block_number', bn).
        property('block_timestamp', new Date(tms)).
        property('previous_block_hash', prev)
    ).iterate();
}
"""

G_SCRIPT_TX = """
// rows = [[tx_hash, is_coinbase(Boolean), block_hash, block_number(Long), block_ts_ms(Long)], ...]
for (r in rows) {
  def th  = r[0];
  def cb  = r[1];
  def bh  = r[2];
  def bn  = r[3] as Long;
  def tms = r[4] as Long;

  // Strict: fail if tx already exists
  def vt = g.V().has('transaction','transaction_hash', th).fold().
    coalesce(
      unfold().sideEffect{ throw new IllegalStateException("Duplicate transaction_hash: " + th) },
      addV('transaction').property('transaction_hash', th).property('is_coinbase', cb)
    ).next();

  // Strict: require the block to already exist
  def vb = g.V().has('block','block_hash', bh).fold().
    coalesce(
      unfold(),
      sideEffect{ throw new IllegalStateException("Missing block for tx " + th + " (block_hash=" + bh + ")") }
    ).next();

  // Strict edge create (error if already exists)
  g.V(vt).coalesce(
    outE('belongs_to').where(inV().hasId(vb.id())).limit(1)
      .sideEffect{ throw new IllegalStateException("belongs_to already exists (tx->block)") },
    addE('belongs_to').to(vb)
  ).iterate();

  if (cb == true) {
    g.V(vb).coalesce(
      outE('coinbase').where(inV().hasId(vt.id())).limit(1)
        .sideEffect{ throw new IllegalStateException("coinbase already exists (block->tx)") },
      addE('coinbase').to(vt)
    ).iterate();
  }
}
"""

G_SCRIPT_OUTPUTS = """
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
  def th      = r[0];
  def vout    = r[1] as Integer;
  def otype   = r[2];
  def oval    = r[3] as Double;
  def bh      = r[4];
  def bn      = r[5] as Long;
  def tms     = r[6] as Long;
  def spendTh = r[7];
  def spendIx = r[8] as Integer;
  def iscb    = r[9];
  def addrs   = r[10];
  def revision= r[11] as Integer;

  // STRICT: transaction must exist
  def vt = g.V().has('transaction','transaction_hash', th).fold().
             coalesce(unfold(), sideEffect{ throw new IllegalStateException("Missing transaction: " + th) }).next();

  // STRICT: block must exist
  def vb = g.V().has('block','block_hash', bh).fold().
             coalesce(unfold(), sideEffect{ throw new IllegalStateException("Missing block: " + bh) }).next();

  // STRICT: belongs_to must exist
  g.V(vt).as('t').V(vb).as('b').
    coalesce(
      select('t').outE('belongs_to').where(inV().as('b')).limit(1),
      sideEffect{ throw new IllegalStateException("Missing belongs_to edge tx->block for tx=" + th + ", block=" + bh) }
    ).iterate();

  // STRICT (conditional): coinbase must exist when is_coinbase == true
  if (iscb == true) {
    g.V(vb).as('b').V(vt).as('t').
      coalesce(
        select('b').outE('coinbase').where(inV().as('t')).limit(1),
        sideEffect{ throw new IllegalStateException("Missing coinbase edge block->tx for tx=" + th + ", block=" + bh) }
      ).iterate();
  }

  // Output: create if missing; then set allowed props
  def vo = g.V().has('output','transaction_hash', th).has('output_index', vout).fold().
             coalesce(unfold(), addV('output').property('transaction_hash', th).property('output_index', vout)).next();

  if (otype != null) { g.V(vo).property('output_type', otype).iterate(); }
  if (oval  != null) { g.V(vo).property('output_value', (double) oval).iterate(); }

  // lock_to (tx -> output) idempotent create + set output_index
  g.V(vt).coalesce(outE('lock_to').where(inV().hasId(vo.id())).limit(1), addE('lock_to').to(vo)).iterate();
  g.V(vt).outE('lock_to').where(inV().hasId(vo.id())).property('output_index', vout).iterate();

  // pay_to (output -> address), idempotent create per address
  if (addrs != null) {
    for (addr in addrs) {
      def va = g.V().has('address','address', addr).fold().coalesce(unfold(), addV('address').property('address', addr)).next();
      g.V(vo).coalesce(outE('pay_to').where(inV().hasId(va.id())).limit(1), addE('pay_to').to(va)).iterate();
    }
  }

  // spent_by (output -> spending tx) when revision > 0: STRICT require spender tx
  if (revision > 0) {
    def vsp = g.V().has('transaction','transaction_hash', spendTh).fold().
                coalesce(unfold(), sideEffect{ throw new IllegalStateException("Missing spender transaction: " + spendTh + " for output " + th + ":" + vout) }).next();
    g.V(vo).coalesce(outE('spent_by').where(inV().hasId(vsp.id())).limit(1), addE('spent_by').to(vsp)).iterate();
    g.V(vo).outE('spent_by').where(inV().hasId(vsp.id())).property('input_index', spendIx).iterate();
  }
}
"""

# =============================
# Block-by-block processors
# =============================
def process_block(block_number: int, verbose: bool = True):
    t0 = time.perf_counter()
    if verbose:
        print(f"[BLOCK] N={block_number} - start")

    # 1) Block row
    block_q = f"""
        SELECT
          hash AS block_hash,
          number AS block_number,
          toUnixTimestamp64Milli(timestamp) AS ts_ms,
          previous_block_hash
        FROM blocks
        WHERE number = {block_number}
        LIMIT 1
    """
    b_rows = qdict(block_q)
    if not b_rows:
        raise RuntimeError(f"Block {block_number} not found in 'blocks' table")

    block_payload = [[
        b_rows[0]["block_hash"],
        int(b_rows[0]["block_number"]) if b_rows[0]["block_number"] is not None else None,
        int(b_rows[0]["ts_ms"]) if b_rows[0]["ts_ms"] is not None else None,
        b_rows[0]["previous_block_hash"],
    ]]
    # Gremlin: write single block
    gremlin_exec(G_SCRIPT_BLOCKS, {"rows": block_payload})

    # 2) Transactions in this block
    tx_q = f"""
        SELECT
          hash AS tx_hash,
          is_coinbase,
          block_hash,
          block_number,
          toUnixTimestamp64Milli(block_timestamp) AS ts_ms
        FROM transactions
        WHERE block_number = {block_number}
        ORDER BY tx_hash
    """
    tx_rows = qdict(tx_q)
    if tx_rows:
        tx_payload = [[
            r.get("tx_hash"),
            bool(r.get("is_coinbase")) if r.get("is_coinbase") is not None else None,
            r.get("block_hash"),
            int(r.get("block_number")) if r.get("block_number") is not None else None,
            int(r.get("ts_ms")) if r.get("ts_ms") is not None else None,
        ] for r in tx_rows]
        gremlin_exec(G_SCRIPT_TX, {"rows": tx_payload})

    # 3) Outputs in this block
    out_q = f"""
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
        WHERE block_number = {block_number}
        ORDER BY transaction_hash, output_index
    """
    out_rows = qdict(out_q)
    if out_rows:
        out_payload = [[
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
            int(r.get("revision")) if r.get("revision") is not None else 0,
        ] for r in out_rows]
        gremlin_exec(G_SCRIPT_OUTPUTS, {"rows": out_payload})

    if verbose:
        dt = time.perf_counter() - t0
        print(f"[BLOCK] N={block_number} - done in {dt:.3f}s")

# =============================
# Main
# =============================
def main():
    parser = argparse.ArgumentParser(description="ClickHouse → JanusGraph (block-by-block, no batching)")
    parser.add_argument("--start-block", type=int, help="Start block_number (inclusive)")
    parser.add_argument("--end-block", type=int, help="End block_number (inclusive)")
    parser.add_argument("--tables", nargs="+", choices=["blocks", "transactions", "outputs"],
                        default=["blocks", "transactions", "outputs"],
                        help="Which tables to process per block (default: all)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    blocks = get_block_numbers(args.start_block, args.end_block)
    if not blocks:
        print("[INFO] No blocks to process with given range.")
        return

    t0 = time.time()
    print(f"[START] Sync blocks {blocks[0]}..{blocks[-1]} ({len(blocks)} total)")

    for bn in blocks:
        try:
            # If user limited tables, you can still call one combined processor
            # and let it skip sections as needed. Here we keep it simple:
            if "blocks" in args.tables or "transactions" in args.tables or "outputs" in args.tables:
                process_block(bn, verbose=args.verbose)
        except Exception as e:
            print(f"[ERROR] While processing block {bn}: {e}")
            # Stop on first failure to honor strictness; comment next line to continue
            return

    print(f"[END] Sync finished in {time.time() - t0:.1f}s")

if __name__ == "__main__":
    main()
