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

def unrich_partition_finding(client, partition):
    query = f"""
    SELECT
        i.transaction_hash          AS transaction_hash,
        i.input_index               AS input_index,
        i.block_hash                AS block_hash,
        i.block_number              AS block_number,
        i.block_timestamp           AS block_timestamp,
        o.required_signatures       AS output_required_signatures,
        i.required_signatures       AS input_required_signatures,
        o.type                      AS output_type,
        i.type                      AS input_type,
        o.addresses                 AS output_addresses,
        i.addresses                 AS input_addresses,
        o.value                     AS output_value,
        i.value                     AS input_value
    FROM inputs AS i
    INNER JOIN outputs AS o
        ON i.spending_transaction_hash = o.transaction_hash
       AND i.spending_output_index = o.output_index
    WHERE toYYYYMM(i.block_timestamp) = {partition}
      AND toYYYYMM(o.block_timestamp) <= {partition}
      AND (
           i.required_signatures != o.required_signatures
           OR i.type != o.type
           OR i.addresses != o.addresses
           OR i.value != o.value
      )
    """
    return client.query(query).result_rows

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
            results = unrich_partition_finding(client, partition)
            if results:
                print(f"❌ Unrich partition {partition} - Mismatches:")
                print(f"Found {len(results)} mismatches in partition {partition}.")
                #print("Mismatched rows:")
                #print("transaction_hash | input_index | block_hash | block_number | block_timestamp | output_required_signatures | input_required_signatures | output_type | input_type | output_addresses | input_addresses | output_value | input_value")
                #print("-" * 100)
                #for row in results:
                #    print(row)
            else:
                print(f"✅ Rich Partition {partition} - No mismatches found.")
        except Exception as e:
            print(f"❌ Failed to optimize partition {partition}: {e}")
            return

if __name__ == "__main__":
    main()
