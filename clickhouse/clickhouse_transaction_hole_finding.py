from clickhouse_connect import get_client
from datetime import datetime
from dateutil.relativedelta import relativedelta

# === CONFIGURATION ===
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'password'
DATABASE = 'bitcoin'

START_MONTH = '2014-10'  # yyyy-mm
END_MONTH   = '2014-11'

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
    else:
        print(f"✅ All transactions found in partition {partition}")

