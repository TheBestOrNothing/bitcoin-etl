import argparse
import clickhouse_connect
from clickhouse_connect import get_client
from typing import Sequence, List

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

def is_transaction_missing(client, partition):
    query = f"""
    WITH flattened AS (
        SELECT
            hash AS block_hash,
            tx_hash,
            number
        FROM bitcoin.blocks_fat
        ARRAY JOIN transactions AS tx_hash
        WHERE toYYYYMM(timestamp) = {partition}
    )
    SELECT
        flattened.number,
        flattened.block_hash,
        flattened.tx_hash
    FROM flattened
    LEFT ANTI JOIN (
        SELECT hash
        FROM bitcoin.transactions_fat
        WHERE toYYYYMM(block_timestamp) = {partition}
    ) AS txs
    ON flattened.tx_hash = txs.hash
    """
    result = client.query(query)
    if result.result_rows:
        return True
    else:
        return False

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

def has_single_active_part(client, partition):
    query = f"""
        SELECT count(*) AS part_count
        FROM system.parts
        WHERE database = '{DATABASE}'
          AND table = 'transactions_fat'
          AND active
          AND partition = '{partition}'
    """
    result = client.query(query)
    return result.result_rows[0][0] <= 1

def has_duplicate_hashes(client, partition):
    query = f"""
        SELECT count(*) > 0
        FROM (
            SELECT hash, count(*) AS c
            FROM transactions_fat
            WHERE toYYYYMM(block_timestamp) = {partition}
            GROUP BY hash
            HAVING c > 1
        )
    """
    result = client.query(query)
    return result.result_rows[0][0] == 1

def is_partition_fully_optimized(client, partition):
    if not has_single_active_part(client, partition):
        return False
    if has_duplicate_hashes(client, partition):
        return False
    return True

def insert_transactions_from_fat(partition: int):
    query = f"""
    INSERT INTO transactions
    SELECT
      hash,
      size,
      virtual_size,
      version,
      lock_time,
      block_hash,
      block_number,
      block_timestamp,
      block_timestamp_month,
      is_coinbase,
      input_count,
      output_count,
      input_value,
      output_value,
      fee,
      inputs,
      outputs,
      0 AS revision
    FROM transactions_fat
    WHERE toYYYYMM(block_timestamp) = {partition}
    """
    client.command(query)

def main():
    parser = argparse.ArgumentParser(description="Deduplicate partitions in blocks_fat from a start partition onward")
    parser.add_argument("--start-partition", default=DEFAULT_START_PARTITION, help="Start partition (YYYYMM)")
    args = parser.parse_args()

    partitions = get_partitions(client, args.start_partition)
    if not partitions:
        print("No partitions found.")
        return

    for partition in partitions:
        try:
            if is_transaction_missing(client, partition):
                print(f"❌️  Missing transaction in partition {partition}")
                return
            if not is_partition_fully_optimized(client, partition):
                print(f"❌ Partition {partition} in transaction_fat is not optimized.")
                return

            insert_transactions_from_fat(partition)
            print(f"✅ Sync the data from transaction_fat to transaction in partition {partition} ")

        except Exception as e:
            print(f"❌ Failed to optimize partition {partition}: {e}")
            return

if __name__ == "__main__":
    main()