import argparse
import clickhouse_connect
from clickhouse_connect import get_client
import time

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
          AND table = 'addresses'
          AND active
          AND partition >= '{start_partition}'
        ORDER BY partition
    """
    result = client.query(query)
    return [row[0] for row in result.result_rows]


def has_duplicate_rows(client, partition):
    query = f"""
    SELECT count() > 0
    FROM
    (
        SELECT
            address,
            transaction_hash,
            output_index
        FROM addresses
        WHERE toYYYYMM(block_timestamp) = {partition}
        GROUP BY address, transaction_hash, output_index
        HAVING count() > 1
        LIMIT 1
    )
    """
    # Execute Query
    result = client.query(query)

    # Extract the boolean (ClickHouse returns 1 or 0)
    exists = result.result_rows[0][0]  # 1 = True, 0 = False

    # Return False if count() > 0, True otherwise
    if exists:
        print(f"Duplicate rows found in partition {partition}.")
        return True
    else:
        print(f"No duplicate rows found in partition {partition}.")
        return False


def optimize_partition_final(client, partition):
    print(f"--- Optimizing partition {partition} in addresses ...")
    query = f"OPTIMIZE TABLE addresses PARTITION '{partition}' FINAL"
    client.command(query)
    print(f"✅ Partition {partition} in addresses optimized.")

def main():
    parser = argparse.ArgumentParser(description="Deduplicate partitions in addresses from a start partition onward")
    parser.add_argument("--start-partition", default=DEFAULT_START_PARTITION, help="Start partition (YYYYMM)")
    args = parser.parse_args()

    partitions = get_partitions(client, args.start_partition)
    if not partitions:
        print("No partitions found.")
        return

    for partition in partitions:
        while True:
            try:
                if not has_duplicate_rows(client, partition):
                    print(f"✓ Partition {partition} in addresses is fully optimized. Skipping.")
                    break

                optimize_partition_final(client, partition)
                break  # Success: exit the while loop and go to next partition

            except Exception as e:
                print(e)
                print(f"⏳ Sleeping 5 minutes before retrying in {partition}...")
                time.sleep(300)

if __name__ == "__main__":
    main()
