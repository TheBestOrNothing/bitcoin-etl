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
          AND table = 'inputs'
          AND active
          AND partition >= '{start_partition}'
        ORDER BY partition
    """
    result = client.query(query)
    return [row[0] for row in result.result_rows]


def check_input_output_consistency(client, partition):
    """
    Checks whether the counts of `inputs`, `inputs_outputs`, and `outputs` tables
    (up to and including the given partition) are equal.
    
    :param partition: Integer partition in YYYYMM format (e.g., 201910)
    :param host: ClickHouse server hostname
    :param port: ClickHouse server port
    :param username: ClickHouse username
    :param password: ClickHouse password
    :return: Tuple (count_inputs, count_inputs_outputs, count_outputs)
    """

    # Format partition value as integer literal (no quotes)
    query_inputs = f"""
        SELECT count()
        FROM inputs FINAL
        WHERE toYYYYMM(block_timestamp) <= {partition}
    """

    query_inputs_outputs = f"""
        SELECT count()
        FROM inputs_outputs
        WHERE toYYYYMM(i_block_timestamp) <= {partition}
    """

    query_outputs = f"""
        SELECT count()
        FROM outputs FINAL
        WHERE toYYYYMM(block_timestamp) <= {partition} AND toYYYYMM(spent_block_timestamp) <= {partition} AND revision = 1
    """

    query_addresses= f"""
        SELECT count()
        FROM addresses FINAL
        WHERE toYYYYMM(block_timestamp) <= {partition} AND toYYYYMM(spent_block_timestamp) <= {partition} AND revision = 1
    """

    # Execute queries
    print(f"Partition: {partition}")
    count_inputs = client.query(query_inputs).result_rows[0][0]
    print("inputs count:         ", count_inputs)
    count_inputs_outputs = client.query(query_inputs_outputs).result_rows[0][0]
    print("inputs_outputs count: ", count_inputs_outputs)
    count_outputs = client.query(query_outputs).result_rows[0][0]
    print("outputs count:        ", count_outputs)
    count_addresses = client.query(query_addresses).result_rows[0][0]
    print("outputs addresses:    ", count_outputs)

    if count_inputs == count_inputs_outputs == count_outputs == count_addresses:
        print("✅ All counts match before partition {partition}.")
        return True
    else:
        print("❌ Mismatch detected in partition {partition}.")
        return False


def main():
    parser = argparse.ArgumentParser(description="Deduplicate partitions in inputs from a start partition onward")
    parser.add_argument("--start-partition", default=DEFAULT_START_PARTITION, help="Start partition (YYYYMM)")
    args = parser.parse_args()

    check_input_output_consistency(client, 202201)
    return

    partitions = get_partitions(client, args.start_partition)
    if not partitions:
        print("No partitions found.")
        return

    for partition in partitions:
        try:
            if not check_input_output_consistency(client, partition):
                return 

        except Exception as e:
            print(e)
            return

if __name__ == "__main__":
    main()
