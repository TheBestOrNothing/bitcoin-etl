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

def hash_missing_blocks(client) -> List[int]:
    """
    Returns a list of missing block numbers across all partitions in the `blocks_fat` table.

    Parameters
    ----------
    client : ClickHouse client
        An already-initialized ClickHouse client (clickhouse-connect or clickhouse-driver).

    Returns
    -------
    list[int]
        Sorted list of missing block numbers across the entire `blocks_fat` table.
    """
    query = """
    WITH seq AS (
        SELECT number
        FROM numbers(
            toUInt64(
                ifNull((SELECT max(number) FROM blocks_fat), 0) + 1
            )
        )
    )
    SELECT seq.number AS missing_block_number
    FROM seq
    LEFT ANTI JOIN blocks_fat AS b ON seq.number = b.number
    ORDER BY missing_block_number
    """

    rows: Sequence[Sequence[int]] = client.query(query).result_rows
    missing_blocks = [row[0] for row in rows]

    if missing_blocks:
        return True
    else:
        return False

def get_partitions(client, start_partition):
    query = f"""
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = '{DATABASE}'
          AND table = 'blocks_fat'
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
          AND table = 'blocks_fat'
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
            FROM blocks_fat 
            WHERE toYYYYMM(timestamp_month) = {partition}
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

def insert_blocks_data(client, partition):
    query = f"""
    INSERT INTO blocks
    SELECT
      hash,
      size,
      stripped_size,
      weight,
      number,
      version,
      merkle_root,
      timestamp,
      timestamp_month,
      nonce,
      bits,
      coinbase_param,
      previous_block_hash,
      difficulty,
      transaction_count,
      transactions,
      0.0 AS total_fees,
      0.0 AS subsidy,
      0.0 AS reward,
      '' AS coinbase_transaction,
      [] AS coinbase_addresses,
      0 AS input_count,
      0.0 AS input_value,
      0 AS output_count,
      0.0 AS output_value,
      0 AS revision
    FROM blocks_fat
    WHERE toYYYYMM(timestamp) = {partition}
    """
    client.query(query)


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
            if hash_missing_blocks(client):
                print(f"❌️  Missing blocks in partition {partition}")
                return
            if not is_partition_fully_optimized(client, partition):
                print(f"❌ Partition {partition} in blocks_fat is not optimized.")
                return
            
            insert_blocks_data(client, partition)

            print(f"✅ Sync the data from blocks_fat to blocks in partition {partition} ")
        except Exception as e:
            print(f"❌ Failed to optimize partition {partition}: {e}")
            return

if __name__ == "__main__":
    main()