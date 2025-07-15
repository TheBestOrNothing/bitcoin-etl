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
DEFAULT_END_PARTITION = '202412'

# === INIT CLIENT ===
client = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE,
)

def get_partitions(client, end_partition):
    query = f"""
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = '{DATABASE}'
          AND table = 'transactions_fat'
          AND active
          AND partition <= '{end_partition}'
        ORDER BY partition
    """
    result = client.query(query)
    return [row[0] for row in result.result_rows]

def truncate_partition(client, partition):
    print(f"Truncating partition {partition} in transactions...")
    query = f"ALTER TABLE transactions_fat DROP PARTITION '{partition}'"
    client.command(query)
    print(f"✅ Partition {partition} truncated in transactions.")

def main():
    parser = argparse.ArgumentParser(description="Deduplicate partitions in transactions from a start partition onward")
    parser.add_argument("--end-partition", default=DEFAULT_END_PARTITION, help="Start partition (YYYYMM)")
    args = parser.parse_args()

    partitions = get_partitions(client, args.end_partition)
    if not partitions:
        print("No partitions found.")
        return

    for partition in partitions:
        while True:
            try:
                truncate_partition(client, partition)
                break  # Success: exit the while loop and go to next partition
            except Exception as e:
                print(e)
                print(f"⏳ Sleeping 10 minutes before retrying in {partition}...")
                time.sleep(600)

if __name__ == "__main__":
    main()
