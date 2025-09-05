from gremlin_python.driver import client, serializer
from typing import Any, Dict, Union
import datetime

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


def ensure_address_vertex(address_str, addr_type=None):
    # print(f"[START] ensure_address_vertex({address_str})")
    if not address_str:
        # print("[END] ensure_address_vertex(None)")
        return None
    props = {'address': address_str}
    if addr_type:
        props['type'] = addr_type
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


def ensure_tx_vertex(tx_hash, block_hash=None, block_number=None, block_timestamp=None, is_coinbase=None):
    # print(f"[START] ensure_tx_vertex({tx_hash})")
    if not tx_hash:
        # print("[END] ensure_tx_vertex(None)")
        return None
    props = {
        'transaction_hash': tx_hash,
        'block_hash': block_hash,
        'block_number': block_number,
        'block_timestamp': block_timestamp,
        'is_coinbase': is_coinbase,
    }
    v = upsert_vertex_by_props('transaction', {'transaction_hash': tx_hash}, props)
    # print(f"[END] ensure_tx_vertex({tx_hash})")
    return v


def ensure_output_vertex(tx_hash, vout, block_hash=None, block_number=None, block_timestamp=None,
                         spent_input_index=None, value=None):
    # print(f"[START] ensure_output_vertex({tx_hash}, {vout})")
    if tx_hash is None or vout is None:
        # print("[END] ensure_output_vertex(None)")
        return None
    props = {
        'transaction_hash': tx_hash,
        'output_index': vout,
        'block_hash': block_hash,
        'block_number': block_number,
        'block_timestamp': block_timestamp,
        'spent_input_index': spent_input_index,
        'value': value,
    }
    v = upsert_vertex_by_props('output',
                               {'transaction_hash': tx_hash, 'output_index': int(vout)},
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
        row.get('block_hash'),
        row.get('block_number'),
        row.get('block_timestamp'),
        row.get('is_coinbase')
    )
    if row.get('revision') == 1:
        v_block_consumer = ensure_block_vertex(
            row.get('spent_block_hash'),
            row.get('spent_block_number'),
            row.get('spent_block_timestamp')
        )
        v_tx_consumer = ensure_tx_vertex(
            row.get('spent_transaction_hash'),
            row.get('spent_block_hash'),
            row.get('spent_block_number'),
            row.get('spent_block_timestamp'),
            False
        )
    v_output = ensure_output_vertex(
        row.get('transaction_hash'),
        row.get('output_index'),
        row.get('block_hash'),
        row.get('block_number'),
        row.get('block_timestamp'),
        row.get('spent_input_index'),
        row.get('value')
    )
    if v_tx_producer and v_block_producer:
        add_edge_unique(v_tx_producer, 'belongs_to', v_block_producer,
                        edge_props={'block_timestamp': row.get('block_timestamp')})
        if row.get('is_coinbase'):
            add_edge_unique(v_block_producer, 'coinbase', v_tx_producer,
                            edge_props={'block_timestamp': row.get('block_timestamp'),
                                        'block_number': row.get('block_number')})
    if v_tx_consumer and v_block_consumer:
        add_edge_unique(v_tx_consumer, 'belongs_to', v_block_consumer,
                        edge_props={'block_timestamp': row.get('spent_block_timestamp')})
    if v_tx_producer and v_output:
        add_edge_unique(v_tx_producer, 'lock_to', v_output,
                        edge_props={'output_index': row.get('output_index'),
                                    'value': row.get('value'),
                                    'block_timestamp': row.get('block_timestamp')})
    if v_output and v_tx_consumer:
        add_edge_unique(v_output, 'spent_in', v_tx_consumer,
                        edge_props={'spent_input_index': row.get('spent_input_index'),
                                    'spent_block_timestamp': row.get('spent_block_timestamp')})
    for addr in row.get('addresses') or []:
        v_addr = ensure_address_vertex(addr, row.get('type'))
        if v_output and v_addr:
            add_edge_unique(v_output, 'pay_to', v_addr,
                            edge_props={'value': row.get('value'),
                                        'block_timestamp': row.get('block_timestamp'),
                                        'type': row.get('type')})
    # print(f"[END] process_inputs_outputs_row(tx={row.get('transaction_hash')}, vout={row.get('output_index')})")


if __name__ == "__main__":
    row = {
        "transaction_hash": "0030800bdbc219ac7089af0798459a209446750f7322a212b496bdfe842184cd",
        "output_index": 0,
        "block_hash": "00000000ffa5e19e744552ad9cf9255de8ac6d4c03b9ed259fca020c9a071b1e",
        "block_number": 688,
        "block_timestamp": "2009-01-16 17:40:02",
        "spent_transaction_hash": "4b96658d39f7fd4241442e6dc9877c8ee0fe0d82477e6014e1681d1efe052c8d",
        "spent_input_index": 0,
        "spent_block_hash": "000000007f6c94c3ad601c4095a9a29abd8fa987972652972079b721b949ba80",
        "spent_block_number": 2754,
        "spent_block_timestamp": "2009-02-03 02:21:36",
        "script_asm": "045e2f...337fa5 OP_CHECKSIG",
        "script_hex": "41045e2f...337fa5ac",
        "required_signatures": 0,
        "type": "nonstandard",
        "addresses": [
            "nonstandard264f01e5d63d95f95705d9a73af50a33472686db",
            "63d95f95705d9a73af50",
            "264f01e5d63d95f9",
            "50a33472686db",
        ],
        "value": 5000000000,
        "is_coinbase": True,
        "revision": 1,
    }
    try:
        process_outputs_row(row)
    finally:
        try:
            gclient.close()
        except Exception:
            pass
