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

def get_partitions(client, start_partition):
    query = f"""
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = '{DATABASE}'
          AND table = 'transactions'
          AND active
          AND partition >= '{start_partition}'
        ORDER BY partition
    """
    result = client.query(query)
    return [row[0] for row in result.result_rows]

def is_transaction_missing(client, partition):
    query = f"""
    WITH flattened AS (
        SELECT
            hash AS block_hash,
            tx_hash,
            number
        FROM bitcoin.blocks
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
        FROM bitcoin.transactions
        WHERE toYYYYMM(block_timestamp) = {partition}
    ) AS txs
    ON flattened.tx_hash = txs.hash
    """
    result = client.query(query)
    if result.result_rows:
        return True
    else:
        return False

def has_single_active_part(client, partition):
    query = f"""
        SELECT count(*) AS part_count
        FROM system.parts
        WHERE database = '{DATABASE}'
          AND table = 'transactions'
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
            FROM transactions
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

def populate_inputs(client, partition):
    # Insert into inputs
    insert_inputs_sql = f"""
    INSERT INTO inputs
    SELECT
        t.hash AS transaction_hash,
        input.1 AS input_index,
        t.block_hash,
        t.block_number,
        t.block_timestamp,
        input.2 AS spending_transaction_hash,
        input.3 AS spending_output_index,
        input.4 AS script_asm,
        input.5 AS script_hex,
        input.6 AS sequence,
        input.7 AS required_signatures,
        input.8 AS type,
        input.9 AS addresses,
        input.10 AS value,
        0 as revision
    FROM transactions AS t
    ARRAY JOIN t.inputs AS input
    WHERE toYYYYMM(t.block_timestamp) = {partition} 
    """
    client.command(insert_inputs_sql)
    print(f"-- Populating inputs for partition {partition}...")


def populate_outputs(client, partition):

    # Insert into outputs (initial)
    insert_outputs_sql = f"""
    INSERT INTO outputs
    SELECT
        t.hash AS transaction_hash,
        output.1 AS output_index,
        t.block_hash,
        t.block_number,
        t.block_timestamp,
        '' AS spent_transaction_hash,
        0 AS spent_input_index,
        '' AS spent_block_hash,
        0 AS spent_block_number,
        toDateTime('1970-01-01 00:00:00') AS spent_block_timestamp,
        output.2 AS script_asm,
        output.3 AS script_hex,
        output.4 AS required_signatures,
        output.5 AS type,
        output.6 AS addresses,
        output.7 AS value,
        t.is_coinbase AS is_coinbase,
        0 AS revision
    FROM transactions AS t
    ARRAY JOIN t.outputs AS output
    WHERE toYYYYMM(t.block_timestamp) = {partition}
    """
    client.command(insert_outputs_sql)
    print(f"-- Populating outputs for partition {partition}...")

def populate_outputs_by_inputs(client, partition):
    # Finalize spent info update
    insert_spent_sql = f"""
    INSERT INTO outputs
    SELECT
        o.transaction_hash,
        o.output_index,
        o.block_hash,
        o.block_number,
        o.block_timestamp,
        i.transaction_hash          AS spent_transaction_hash,
        i.input_index               AS spent_input_index,
        i.block_hash                AS spent_block_hash,
        i.block_number              AS spent_block_number,
        i.block_timestamp           AS spent_block_timestamp,
        o.script_asm,
        o.script_hex,
        o.required_signatures,
        o.type,
        o.addresses,
        o.value,
        o.is_coinbase,
        1 AS revision
    FROM inputs AS i
    INNER JOIN outputs AS o
        ON i.spending_transaction_hash = o.transaction_hash
    AND i.spending_output_index = o.output_index
    WHERE toYYYYMM(i.block_timestamp) = {partition}
    """
    client.command(insert_spent_sql)
    print(f"-- Populating outputs by inputs for partition {partition}...")


def populate_inputs_by_outputs(client, partition):
    # Finalize spent info update
    insert_spent_sql = f"""
    INSERT INTO inputs
    SELECT
        i.transaction_hash,
        i.input_index,
        i.block_hash,
        i.block_number,
        i.block_timestamp,
        i.spending_transaction_hash,
        i.spending_output_index,
        i.script_asm,
        i.script_hex,
        i.sequence,
        o.required_signatures AS required_signatures,
        o.type AS type,
        o.addresses AS addresses,
        o.value AS value,
        1 AS revision
    FROM inputs AS i
    INNER JOIN outputs AS o
        ON i.spending_transaction_hash = o.transaction_hash
    AND i.spending_output_index = o.output_index
    WHERE toYYYYMM(i.block_timestamp) = {partition}
    """
    client.command(insert_spent_sql)
    print(f"-- Populating inputs by outputs for partition {partition}...")

def main():
    parser = argparse.ArgumentParser(description="Deduplicate partitions in blocks from a start partition onward")
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
            if has_duplicate_hashes(client, partition):
                print(f"❌ Partition {partition} in transactions is not optimized.")
                return

            populate_inputs(client, partition)
            populate_outputs(client, partition)
            populate_outputs_by_inputs(client, partition)
            populate_inputs_by_outputs(client, partition)

        except Exception as e:
            print(f"Sync inputs and outputs error when processing partition {partition}: {e}")

if __name__ == "__main__":
    main()
