from clickhouse_connect import get_client
from datetime import datetime
from dateutil.relativedelta import relativedelta

from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc

from blockchainetl.thread_local_proxy import ThreadLocalProxy
from bitcoinetl.streaming.streaming_utils import get_item_exporter
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter
from blockchainetl.streaming.streamer import Streamer


def fix_block_hole(missing_blocks):
    """Fixes a hole in the blockchain by exporting a specific block."""

    provider_uri = 'http://bitcoin:passw0rd@localhost:8332'
    output = 'kafka/localhost:9092'
    chain = Chain.BITCOIN
    batch_size = 1
    enrich = False
    max_workers = 1

    streamer_adapter = BtcStreamerAdapter(
        bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
        item_exporter=get_item_exporter(output),
        chain=chain,
        batch_size=batch_size,
        enable_enrich=enrich,
        max_workers=max_workers
    )
    
    streamer_adapter.open()
    for block_number in missing_blocks:
        # Export the specific block
        try:
            streamer_adapter.export_all(start_block=block_number, end_block=block_number)
            print(f"Fixed Block: {block_number}")
        except Exception as e:
            print(f"Error fixing block {block_number}: {e}")

    streamer_adapter.close()


# === CONFIGURATION ===
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'password'
DATABASE = 'bitcoin'

START_MONTH = '2015-08'  # yyyy-mm
END_MONTH   = '2016-01'

# === INIT CLIENT ===
client = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE,
)

# === GENERATE PARTITIONS ===
def generate_partitions(start, end):
    partitions = []
    start_date = datetime.strptime(start, '%Y-%m')
    end_date = datetime.strptime(end, '%Y-%m')
    current = start_date
    while current <= end_date:
        partitions.append(current.strftime('%Y%m'))
        current += relativedelta(months=1)
    return partitions

# === MAIN LOOP ===
partitions = generate_partitions(START_MONTH, END_MONTH)

for partition in partitions:
    print(f'\n🧪 Querying Partition: {partition}')
    query = f"""
    WITH flattened AS (
        SELECT
            hash AS block_hash,
            tx_hash,
            number
        FROM bitcoin.blocks_fat
        ARRAY JOIN transactions AS tx_hash
        WHERE toYYYYMM(timestamp_month) = {partition}
    )
    SELECT
        flattened.number,
        flattened.block_hash,
        flattened.tx_hash
    FROM flattened
    LEFT ANTI JOIN (
        SELECT hash
        FROM bitcoin.transactions_fat
        WHERE toYYYYMM(block_timestamp_month) = {partition}
    ) AS txs
    ON flattened.tx_hash = txs.hash
    """
    result = client.query(query)
    rows = result.result_rows
    if rows:
        print(f"⚠️  Missing transactions in partition {partition}: {len(rows)}")
        for row in rows:
            print(f"    Block: {row[0]}  Missing TX: {row[2]}")
        # Fix the hole by exporting the missing transactions
        missing_blocks = {row[0] for row in rows}
        fix_block_hole(missing_blocks)
    else:
        print(f"✅ All transactions found in partition {partition}")

