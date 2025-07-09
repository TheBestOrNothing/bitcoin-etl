from clickhouse_connect import get_client
from datetime import datetime
from dateutil.relativedelta import relativedelta

from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc

from blockchainetl.thread_local_proxy import ThreadLocalProxy
from bitcoinetl.streaming.streaming_utils import get_item_exporter
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter
from blockchainetl.streaming.streamer import Streamer
import argparse


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

DEFAULT_START_PARTITION = '200901'

# === INIT CLIENT ===
client = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE,
)

# === INIT CLIENT ===
client = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE,
)

def get_partitions(client, start_partition):
    query = f"""
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = '{DATABASE}'
          AND table = 'transactions_fat'
          AND active
          AND partition >= '{start_partition}'
        ORDER BY partition
    """
    result = client.query(query)
    return [row[0] for row in result.result_rows]

def transaction_hole_finding(client, partition):
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
    return result.result_rows

def main():
    parser = argparse.ArgumentParser(description="Deduplicate partitions in transactions_fat from a start partition onward")
    parser.add_argument("--start-partition", default=DEFAULT_START_PARTITION, help="Start partition (YYYYMM)")
    args = parser.parse_args()

    partitions = get_partitions(client, args.start_partition)
    if not partitions:
        print("No partitions found.")
        return

    for partition in partitions:
        try:
            rows = transaction_hole_finding(client, partition)
            if rows:
                print(f"⚠️  Missing transactions in partition {partition}: {len(rows)}")
                for row in rows:
                    print(f"    Block: {row[0]}  Missing TX: {row[2]}")
                # Fix the hole by exporting the missing transactions
                missing_blocks = {row[0] for row in rows}
                fix_block_hole(missing_blocks)
            else:
                print(f"✅ All transactions found in partition {partition}")
        except Exception as e:
            print(f"❌ Exception when fixing transaction hole in partition {partition}: {e}")

if __name__ == "__main__":
    main()