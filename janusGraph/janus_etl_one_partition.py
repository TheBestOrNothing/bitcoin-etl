from gremlin_python.driver import client, serializer
from clickhouse_connect import get_client
import os
from typing import List, Dict, Any
import time

# Batch sizes (tune to your infra)
#READ_LIMIT = int(os.getenv("READ_LIMIT", "5000"))       # fetch from ClickHouse per chunk
READ_LIMIT = int(os.getenv("READ_LIMIT", "400"))       # fetch from ClickHouse per chunk
GREMLIN_BATCH = int(os.getenv("GREMLIN_BATCH", "1000")) # send to Gremlin per chunk

# =============================
# ClickHouse client
# =============================
# === CONFIGURATION ===
CLICKHOUSE_HOST = '192.168.2.65'
#CLICKHOUSE_HOST = 'localhost'

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

def gremlin_exec(gclient, script: str, bindings: Dict[str, Any], *, retries: int = 1):
    """
    Submit with a per-request evaluation timeout and a single reconnect retry
    if the connection was already closed by the server.
    """
    try:
        return gclient.submit(
            script,
            bindings=bindings,
            request_options={'evaluationTimeout': GREMLIN_REQUEST_TIMEOUT_MS}
        ).all().result()
    except Exception as e:
        msg = str(e)
        if retries > 0 and ("Connection was already closed" in msg or "WebSocket" in msg):
            print(f"[WARN] Gremlin connection dropped: {msg}. Reconnecting and retrying once...")
            try:
                gclient.close()
            except Exception:
                pass
            gclient = make_gclient()
            return gremlin_exec(gclient,script, bindings, retries=retries-1)
        raise

def fetch_rows_in_chunks(query: str, params: Dict[str, Any] = None, limit: int = READ_LIMIT):
    """
    Yields rows (as dicts) in chunks from ClickHouse using LIMIT/OFFSET pagination.
    """
    offset = 0
    while True:
        q = f"{query} LIMIT {limit} OFFSET {offset}"
        rs = ch.query(q, parameters=params or {})
        cols = rs.column_names
        rows = [dict(zip(cols, r)) for r in rs.result_rows]
        if not rows:
            break
        yield rows
        offset += limit

# =============================
# Blocks sync
# =============================
def sync_blocks_partition(gclient, part_yyyymm: str):
    G_SCRIPT_BLOCKS = """
    // rows = [[block_hash, block_number(Long), block_ts_sec(Long), prev_block_hash?], ...]
    for (r in rows) {
    def bh = r[0]
    def bn = r[1] as Long
    // def tms = (r[2] as Long) * 1000L
    def tms = r[2] as long
    def prev = (r.size() > 3 ? r[3] : null)

    g.V().has('block','block_hash', bh).fold().
        coalesce(
        unfold(),
        addV('block').property('block_hash', bh)
                    .property('block_number', bn)
                    .property('block_timestamp', new Date(tms))
                    .property('previous_block_hash', prev)
        ).
        property('block_number', bn).
        property('block_timestamp', new Date(tms)).
        property('previous_block_hash', prev).
        iterate()
    }
    """

    base_q = f"""
    SELECT
        hash AS block_hash,
        number AS block_number,
        toUnixTimestamp(timestamp) AS ts_ms,
        previous_block_hash
    FROM blocks
    WHERE toYYYYMM(timestamp) = {202506} 
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
        print(f"Sending {len(payload)} blocks to gremlin...")
        gclient.submit(G_SCRIPT_BLOCKS, {"rows": payload}).all().result()
        #gremlin_exec(gclient, G_SCRIPT_BLOCKS, {"rows": payload})

    print(f"[BLOCKS] Done partition 202506")



# =============================
# Transactions sync
# =============================
def sync_transactions_partition(part_yyyymm: str):
    print(f"[TX] Start partition {part_yyyymm}")
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
        unfold(),
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
        outE('belongs_to').where(inV().hasId(vb.id())).limit(1),
        addE('belongs_to').to(vb)
    ).iterate()

    if (cb == true) {
        g.V(vb).coalesce(
        outE('coinbase').where(inV().hasId(vt.id())).limit(1),
        addE('coinbase').to(vt)
        ).iterate()
    }
    }
    """


    base_q = f"""
    SELECT
      hash AS tx_hash,
      is_coinbase,
      block_hash,
      block_number,
      toUnixTimestamp(block_timestamp) AS ts_ms
    FROM transactions
    WHERE toYYYYMM(block_timestamp) = {part_yyyymm}
    ORDER BY tx_hash
    """
    for rows in fetch_rows_in_chunks(base_q, None, 200):
        payload = []
        for r in rows:
            payload.append([
                r.get("tx_hash"),
                bool(r.get("is_coinbase")) if r.get("is_coinbase") is not None else None,
                r.get("block_hash"),
                int(r.get("block_number")) if r.get("block_number") is not None else None,
                int(r.get("ts_ms")) if r.get("ts_ms") is not None else None,
            ])
        gclient.submit(G_SCRIPT_TX, {"rows": payload}).all().result()
    print(f"[TX] Done partition {part_yyyymm}")


# =============================
# Outputs sync
# =============================
def sync_outputs_partition(gclient, part_yyyymm: str):
    print(f"[OUT] Start partition {part_yyyymm}")
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

    base_q = f"""
    SELECT
      transaction_hash,
      toInt32(output_index) AS output_index,
      type,
      value,
      block_hash,
      block_number,
      toUnixTimestamp(block_timestamp) AS ts_ms,
      spent_transaction_hash,
      spent_input_index,
      is_coinbase,
      addresses,
      revision
    FROM outputs
    WHERE toYYYYMM(block_timestamp) = {part_yyyymm}
    ORDER BY transaction_hash, output_index
    """
    for rows in fetch_rows_in_chunks(base_q, None, 100):
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
        gclient.submit(G_SCRIPT_OUTPUTS, {"rows": payload}).all().result()
    print(f"[OUT] Done partition {part_yyyymm}")

if __name__ == "__main__":
    #sync_blocks_partition(gclient, "202506")
    #sync_transactions_partition("202506")
    sync_outputs_partition(gclient, "202506")
    gclient.close()


