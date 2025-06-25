from clickhouse_connect import get_client
from datetime import datetime
from dateutil.relativedelta import relativedelta

from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.jobs.enrich_transactions import EnrichTransactionsJob

from blockchainetl.thread_local_proxy import ThreadLocalProxy
from bitcoinetl.streaming.streaming_utils import get_item_exporter
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter
from blockchainetl.streaming.streamer import Streamer
from bitcoinetl.service.btc_service import BtcService


def fix_transaction_hole(missing_transactions_hashes):
    """Fixes a hole in the blockchain by exporting specific transactions."""
    
    provider_uri = 'http://bitcoin:passw0rd@localhost:8332'
    output = 'kafka/localhost:9092'
    chain = Chain.BITCOIN
    batch_size = 1
    enrich = True
    max_workers = 1

    bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri))
    btc_service = BtcService(bitcoin_rpc, chain)

    # missing_transactions = btc_service.get_transactions_by_hashes(missing_transactions_hashes)
    missing_transactions = btc_service._get_raw_transactions_by_hashes_batched(missing_transactions_hashes)
    print(f"Found {len(missing_transactions)} missing transactions to enrich.")
    # print("missing transaction 0", missing_transactions[0])

    enrich_transaction = EnrichTransactionsJob(
        transactions_iterable=missing_transactions,
        batch_size=batch_size,
        bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
        max_workers=max_workers,
        item_exporter=get_item_exporter(output),
        chain=chain
    )

    # Start the job to enrich transactions
    enrich_transaction._start()
    enrich_transaction._export()
    enrich_transaction._end()




    
# === CONFIGURATION ===
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'password'
DATABASE = 'bitcoin'

START_MONTH = '2015-01'  # yyyy-mm
END_MONTH   = '2015-01'

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
            print(f"    Block: {row[0]}  Block Hash: {row[1]}  Missing TX: {row[2]}")
        # Fix the hole by exporting the missing transactions
        # Get all transactions for the missing TX hashes
        missing_transactions = [row[2] for row in rows]
        fix_transaction_hole(missing_transactions)
    else:
        print(f"✅ All transactions found in partition {partition}")

