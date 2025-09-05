#!/usr/bin/env python3
from typing import Any, Dict, Union
from gremlin_python.driver import client, serializer
import datetime
import argparse
from typing import Optional, Dict, Any, Tuple
from clickhouse_connect import get_client


# ---- connection ----
GREMLIN_ENDPOINT = "ws://192.168.2.65:8182/gremlin"
gclient = client.Client(
    GREMLIN_ENDPOINT,
    'g',
    message_serializer=serializer.GraphSONSerializersV3d0()
)

# === Schema-aware allowlists (match your mgmt schema) ===
ALLOWED_VERTEX_PROPS = {
    'block':        {'block_hash', 'block_number', 'block_timestamp'},
    'transaction':  {'transaction_hash', 'block_hash', 'block_number', 'block_timestamp', 'is_coinbase'},
    'output':       {'transaction_hash', 'output_index', 'block_hash', 'block_number', 'block_timestamp', 'spent_input_index', 'value'},
    'address':      {'address', 'type'},
}

# Edge properties that your schema keys can support (we use only keys you created):
ALLOWED_EDGE_PROPS = {
    'belongs_to': {'block_timestamp'},  # transaction -> block
    'coinbase':   {'block_timestamp', 'block_number'},  # block -> transaction
    'lock_to':    {'output_index', 'value', 'block_timestamp'},  # transaction -> output
    'pay_to':     {'value', 'block_timestamp', 'type'},  # output -> address
    'spent_in':   {'spent_input_index', 'spent_block_timestamp'},  # output -> transaction
}

# ---- helpers ----
def to_epoch_seconds(dt):
    # print(f"[START] to_epoch_seconds({dt})")
    if dt is None:
        # print("[END] to_epoch_seconds(None)")
        return None
    if isinstance(dt, (int, float)):
        # print("[END] to_epoch_seconds(number)")
        return int(dt)
    if isinstance(dt, datetime.datetime):
        # print("[END] to_epoch_seconds(datetime)")
        return int(dt.timestamp())
    try:
        val = int(datetime.datetime.fromisoformat(str(dt)).timestamp())
        # print("[END] to_epoch_seconds(string->epoch)")
        return val
    except Exception:
        # print("[END] to_epoch_seconds(failed)")
        return None


def cast_for_schema(label, props):
    out = {}
    allowed = ALLOWED_VERTEX_PROPS.get(label, set())
    for k in list(props.keys()):
        if k not in allowed:
            continue
        v = props[k]
        if v is None:
            continue
        if k in ('output_index', 'spent_input_index'):
            out[k] = int(v)
        elif k in ('block_number',):
            out[k] = int(v)
        elif k in ('block_timestamp',):
            out[k] = to_epoch_seconds(v)
        elif k in ('value',):
            out[k] = float(v)
        elif k in ('is_coinbase',):
            out[k] = bool(v)
        else:
            out[k] = v
    return out


def cast_edge_props(label, edge_props):
    out = {}
    allowed = ALLOWED_EDGE_PROPS.get(label, set())
    for k, v in (edge_props or {}).items():
        if k not in allowed or v is None:
            continue
        if k in ('output_index', 'spent_input_index', 'block_number'):
            out[k] = int(v)
        elif k in ('block_timestamp', 'spent_block_timestamp'):
            out[k] = to_epoch_seconds(v)
        elif k == 'value':
            out[k] = float(v)
        else:
            out[k] = v
    return out

def upsert_vertex_by_props(label, key_props, props):
    # print(f"[START] upsert_vertex_by_props(label={label}, keys={key_props})")
    clean_props = cast_for_schema(label, props)

    if not key_props:
        raise ValueError("key_props must contain at least one unique identifying key")

    bindings = {
        'vlabel': label,
        'kmap': key_props,
        'pprops': clean_props
    }

    script = """
    def q = g.V().hasLabel(vlabel)
    kmap.each { k, v -> q = q.has(k, v) }
    def firstK = kmap.keySet().iterator().next()
    def firstV = kmap[firstK]
    q.fold().
      coalesce(
        unfold(),
        addV(vlabel).property(firstK, firstV)
      ).
      sideEffect { t ->
        def vtx = t.get()
        pprops.each { k, v -> vtx.property(k, v) }
      }.
      limit(1)
    """

    result = gclient.submit(script, bindings).all().result()
    # print(f"[END] upsert_vertex_by_props(label={label}, keys={key_props})")
    return result[0] if result else None


def _vertex_id(x: Any) -> Any:
    if hasattr(x, 'id'):
        return x.id
    if isinstance(x, dict) and 'id' in x:
        return x['id']
    return x

def add_edge_unique(out_v: Union[dict, Any], label: str, in_v: Union[dict, Any], edge_props: Dict[str, Any] = None):
    eprops = cast_edge_props(label, edge_props or {})
    out_id = _vertex_id(out_v)
    in_id  = _vertex_id(in_v)

    # print(f"[START] add_edge_unique({label}, out={out_id}, in={in_id})")

    bindings = {
        'outId': out_id,
        'inId': in_id,
        'elabel': label,
        'eprops': eprops
    }

    script = """
    g.V(outId).as('a').V(inId).as('b').
      coalesce(
        select('a').outE(elabel).where(inV().hasId(inId)).limit(1),
        addE(elabel).from('a').to('b')
      ).
      sideEffect { t ->
        def e = t.get()
        eprops.each { k, v -> e.property(k, v) }
      }.
      limit(1)
    """

    gclient.submit(script, bindings).all().result()
    # print(f"[END] add_edge_unique({label}, out={out_id}, in={in_id})")


def ensure_address_vertex(address_str):
    # print(f"[START] ensure_address_vertex({address_str})")
    if not address_str:
        # print("[END] ensure_address_vertex(None)")
        return None
    props = {
        'address': address_str,
    }
    v = upsert_vertex_by_props('address', {'address': address_str}, props)
    # print(f"[END] ensure_address_vertex({address_str})")
    return v


def ensure_block_vertex(block_hash, block_number=None, block_timestamp=None):
    # print(f"[START] ensure_block_vertex({block_hash})")
    if not block_hash:
        # print("[END] ensure_block_vertex(None)")
        return None
    props = {
        'block_hash': block_hash,
        'block_number': block_number,
        'block_timestamp': block_timestamp,
    }
    v = upsert_vertex_by_props('block', {'block_hash': block_hash}, props)
    # print(f"[END] ensure_block_vertex({block_hash})")
    return v


def ensure_tx_vertex(tx_hash, is_coinbase=None):
    # print(f"[START] ensure_tx_vertex({tx_hash})")
    if not tx_hash:
        # print("[END] ensure_tx_vertex(None)")
        return None
    props = {
        'transaction_hash': tx_hash,
        'is_coinbase': is_coinbase,
    }
    v = upsert_vertex_by_props('transaction', {'transaction_hash': tx_hash}, props)
    # print(f"[END] ensure_tx_vertex({tx_hash})")
    return v


def ensure_output_vertex(tx_hash, output_index, lock_type=None, value=None):
    # print(f"[START] ensure_output_vertex({tx_hash}, {vout})")
    if tx_hash is None or output_index is None:
        # print("[END] ensure_output_vertex(None)")
        return None
    props = {
        'transaction_hash': tx_hash,
        'output_index': int(output_index),
        'type': lock_type,
        'value': value,
    }
    v = upsert_vertex_by_props('output',
                               {'transaction_hash': tx_hash, 'output_index': int(output_index)},
                               props)
    # print(f"[END] ensure_output_vertex({tx_hash}, {vout})")
    return v


def process_outputs_row(row):
    # print(f"[START] process_inputs_outputs_row(tx={row.get('transaction_hash')}, vout={row.get('output_index')})")
    v_block_producer = ensure_block_vertex(
        row.get('block_hash'),
        row.get('block_number'),
        row.get('block_timestamp')
    )

    v_tx_producer = ensure_tx_vertex(
        row.get('transaction_hash'),
        row.get('is_coinbase')
    )

    v_output = ensure_output_vertex(
        row.get('transaction_hash'),
        row.get('output_index'),
        row.get('type'),
        row.get('value')
    )

    if v_tx_producer and v_block_producer:
        add_edge_unique(v_tx_producer, 'belongs_to', v_block_producer)
        if row.get('is_coinbase'):
            add_edge_unique(v_block_producer, 'coinbase', v_tx_producer)

    if v_tx_producer and v_output:
        add_edge_unique(v_tx_producer, 'lock_to', v_output,
                        edge_props={'output_index': row.get('output_index')})

    for addr in row.get('addresses') or []:
        v_addr = ensure_address_vertex(addr)
        if v_output and v_addr:
            add_edge_unique(v_output, 'pay_to', v_addr)
 
    if row.get('revision') == 1:
        v_block_consumer = ensure_block_vertex(
            row.get('spent_block_hash'),
            row.get('spent_block_number'),
            row.get('spent_block_timestamp')
        )
        v_tx_consumer = ensure_tx_vertex(
            row.get('spent_transaction_hash'),
            False
        )

        if v_tx_consumer and v_block_consumer:
            add_edge_unique(v_tx_consumer, 'belongs_to', v_block_consumer)

        if v_output and v_tx_consumer:
            add_edge_unique(v_output, 'spent_in', v_tx_consumer,
                            edge_props={'input_index': row.get('spent_input_index')})

   # print(f"[END] process_inputs_outputs_row(tx={row.get('transaction_hash')}, vout={row.get('output_index')})")


# === CLICKHOUSE CONFIG ===
CLICKHOUSE_HOST = '192.168.2.65'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'password'
DATABASE = 'bitcoin'
DEFAULT_START_PARTITION = '200901'
DEFAULT_BATCH_SIZE = 50_000

# ---- bring your ETL function into scope ----
# from your_gremlin_module import process_outputs_row
# Here we assume process_outputs_row(row: Dict[str, Any]) is already defined in scope.


def get_partitions(client, start_partition: str):
    """
    Return sorted list of active partitions (YYYYMM strings) for `bitcoin.outputs`
    starting at start_partition inclusive.
    """
    query = f"""
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = '{DATABASE}'
          AND table = 'outputs'
          AND active
          AND partition >= '{start_partition}'
        ORDER BY partition
    """
    result = client.query(query)
    return [row[0] for row in result.result_rows]


def fetch_batch(client,
                partition: str,
                last_key: Optional[Tuple[str, int]],
                batch_size: int):
    """
    Keyset-paginate outputs within a partition ordered by (transaction_hash, output_index).
    last_key is a tuple (last_tx_hash, last_vout) from previous batch.
    Returns (rows, new_last_key, done)
    """
    # WHERE toYYYYMM(block_timestamp) = CAST(YYYYMM AS UInt32) binds us to the partition
    where_partition = f"toYYYYMM(block_timestamp) = toUInt32('{partition}')"

    # Keyset continuation condition
    if last_key is None:
        where_keyset = "1"
    else:
        last_tx, last_vout = last_key
        # (tx, vout) > (last_tx, last_vout)
        where_keyset = (
            f"(transaction_hash > '{last_tx}') OR "
            f"(transaction_hash = '{last_tx}' AND output_index > {last_vout})"
        )

    query = f"""
        SELECT
            transaction_hash,
            output_index,
            block_hash,
            block_number,
            block_timestamp,
            spent_transaction_hash,
            spent_input_index,
            spent_block_hash,
            spent_block_number,
            spent_block_timestamp,
            script_asm,
            script_hex,
            required_signatures,
            type,
            addresses,
            value,
            is_coinbase,
            revision
        FROM {DATABASE}.outputs
        WHERE {where_partition}
          AND ({where_keyset})
        ORDER BY transaction_hash ASC, output_index ASC
        LIMIT {batch_size}
    """
    rs = client.query(query)
    cols = rs.column_names
    rows = [dict(zip(cols, r)) for r in rs.result_rows]

    if not rows:
        return [], last_key, True

    # New last key = last row's (tx, vout)
    last_row = rows[-1]
    new_last_key = (last_row['transaction_hash'], int(last_row['output_index']))
    done = len(rows) < batch_size
    return rows, new_last_key, done


def main():
    parser = argparse.ArgumentParser(
        description="Scan bitcoin.outputs partition-by-partition and process each row with process_outputs_row"
    )
    parser.add_argument("--start-partition", default=DEFAULT_START_PARTITION,
                        help="Start partition (YYYYMM), e.g., 200901")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Batch size per query (default {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--verbose", action="store_true",
                        help="Print per-partition progress counters")
    args = parser.parse_args()

    client = get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=DATABASE,
    )

    try:
        partitions = get_partitions(client, args.start_partition)
        if not partitions:
            # silent failure by default; flip on --verbose if you want logs
            if args.verbose:
                print("No partitions found.")
            return

        total_processed = 0
        for p in partitions:
            part_count = 0
            last_key = None
            done = False
            if args.verbose:
                print(f"[PARTITION {p}] start")

            while not done:
                rows, last_key, done = fetch_batch(client, p, last_key, args.batch_size)

                for row in rows:
                    # Ensure Python-native types are acceptable to your ETL:
                    # clickhouse_connect returns datetime for DateTime, list for Array, etc.
                    try:
                        process_outputs_row(row)
                        part_count += 1
                        total_processed += 1
                    except Exception as e:
                        # Fail soft per row; you can log or send to DLQ here
                        if args.verbose:
                            print(f"[WARN] row failed in partition {p}: {e}")

                if args.verbose:
                    print(f"[PARTITION {p}] processed so far: {part_count}")

            if args.verbose:
                print(f"[PARTITION {p}] done. processed={part_count}")

        if args.verbose:
            print(f"[ALL] done. total_processed={total_processed}")

    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
