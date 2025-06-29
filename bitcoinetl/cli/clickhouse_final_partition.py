import argparse
import clickhouse_connect
from clickhouse_connect import get_client

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
            WHERE toYYYYMM(block_timestamp_month) = {partition}
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

def optimize_partition_final(client, partition):
    print(f">>> Checking partition {partition}...")
    if is_partition_fully_optimized(client, partition):
        print(f"✓ Partition {partition} is fully optimized. Skipping.")
        return
    print(f"⏳ Optimizing partition {partition}...")
    query = f"OPTIMIZE TABLE transactions_fat PARTITION '{partition}' FINAL"
    client.command(query)
    print(f"✅ Partition {partition} optimized.")

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
            optimize_partition_final(client, partition)
        except Exception as e:
            print(f"❌ Failed to optimize partition {partition}: {e}")

if __name__ == "__main__":
    main()
