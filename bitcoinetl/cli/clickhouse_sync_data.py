import argparse
import clickhouse_connect

# Example parameters
host = "localhost"
# host = "192.168.2.242"
# host = "host.docker.internal"
port = 8123
user = "default"
password = "password"  # Empty by default
# password = ""  # Empty by default
dbname = "bitcoin"

client = clickhouse_connect.get_client(
    host=host,
    port=port,
    username=user,
    password=password,
    database=dbname
)

# Run a test query
result = client.query("SELECT count() from blocks_fat")
print(result.result_rows)

def find_missing_block_partition(client):
    """
    Finds the first missing block number and its preceding partition.

    Returns:
        int or None: The YYYYMM of the partition before the missing block, or None if no missing block.
    """
    missing_block_query = """
    WITH seq AS (
        SELECT number
        FROM numbers(
            toUInt64(
                ifNull((SELECT max(number) FROM blocks_fat), 0) + 1
            )
        )
    )
    SELECT min(seq.number) AS missing_block_number
    FROM seq
    LEFT ANTI JOIN blocks_fat AS b ON seq.number = b.number
    """
    result = client.query(missing_block_query).result_rows
    if result and result[0][0] is not None:
        missing_block = result[0][0]
        print('missing_block', missing_block)
        block_before_missing = missing_block - 1
        partition_query = f"""
        SELECT toYYYYMM(timestamp_month)
        FROM blocks_fat
        WHERE number = {block_before_missing}
        """
        partition_result = client.query(partition_query).result_rows
        return partition_result[0][0] if partition_result else None
    return None


def get_block_partitions(client):
    """
    Queries all unique YYYYMM partitions from the `blocks_fat` table.
    """
    query = """
        SELECT DISTINCT toYYYYMM(timestamp_month) AS part
        FROM blocks_fat
        ORDER BY part
    """
    partitions = client.query(query).result_rows
    return [row[0] for row in partitions]


def check_partition_for_holes(partition: int, client):
    """
    Checks a specific partition for transaction mismatches.
    Prints info if any mismatch is found.
    """
    block_tx_missing_query = f"""
    WITH flattened AS (
        SELECT
            hash AS block_hash,
            arrayJoin(transactions) AS tx_hash,
            number As block_number
        FROM blocks_fat 
        WHERE toYYYYMM(timestamp_month) = {partition}
    )
    SELECT
        flattened.block_hash,
        flattened.tx_hash,
        flattened.block_number
    FROM flattened
    LEFT ANTI JOIN (
        SELECT hash
        FROM transactions_fat 
        WHERE toYYYYMM(block_timestamp_month) = {partition}
    ) AS t
    ON flattened.tx_hash = t.hash
    """
    missing_in_transactions = client.query(block_tx_missing_query).result_rows

    tx_not_in_blocks_query = f"""
    WITH flattened AS (
        SELECT
            hash AS block_hash,
            arrayJoin(transactions) AS tx_hash
        FROM blocks_fat 
        WHERE toYYYYMM(timestamp_month) = {partition}
    )
    SELECT
        t.hash
    FROM (
        SELECT hash
        FROM transactions_fat 
        WHERE toYYYYMM(block_timestamp_month) = {partition}
    ) AS t
    LEFT ANTI JOIN flattened
    ON t.hash = flattened.tx_hash
    """
    missing_in_blocks = client.query(tx_not_in_blocks_query).result_rows

    if missing_in_transactions:
        print(f"\n❌ Partition {partition}: block.transactions missing in transactions_fat")
        for row in missing_in_transactions:
            print(f" - Transaction {row[1]} is in block {row[2]} but missing from transactions_fat")
        return True

    if missing_in_blocks:
        print(f"\n❌ Partition {partition}: transactions_fat entries missing in block.transactions")
        for row in missing_in_blocks:
            print(f" - Transaction {row[0]} exists in transactions_fat but not in any blocks_fat.transactions")
        return True

    print(f"-- Partition {partition}: No transaction holes found")
    return False

def final_transactions_fat(partition: int, client):
    """
    Optimizes the specified partition of the transactions_fat table using FINAL.

    Args:
        partition (int): The partition value in YYYYMM format (e.g., 202304).
        client: ClickHouse client instance.
    """
    sql = f"OPTIMIZE TABLE transactions_fat PARTITION {partition} FINAL"
    client.command(sql)
    print(f"-- Optimizing transactions_fat partition {partition}...")


def populate_inputs(partition: int, client):
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
        input.10 AS value
    FROM (
        SELECT
            hash,
            block_hash,
            block_number,
            block_timestamp,
            inputs
        FROM transactions_fat 
        WHERE toYYYYMM(block_timestamp) = {partition}
    ) AS t
    ARRAY JOIN t.inputs AS input
    """
    client.command(insert_inputs_sql)
    print(f"-- Populating inputs for partition {partition}...")


def populate_outputs(partition: int, client):

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
        output.7 AS value
    FROM (
        SELECT
            hash,
            block_hash,
            block_number,
            block_timestamp,
            outputs
        FROM transactions_fat 
        WHERE toYYYYMM(block_timestamp) = {partition}
    ) AS t
    ARRAY JOIN t.outputs AS output
    """
    client.command(insert_outputs_sql)
    print(f"-- Populating outputs for partition {partition}...")

def populate_outputs_by_inputs(partition: int, client):
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
        o.value
    FROM (
        SELECT
            transaction_hash,
            input_index,
            block_hash,
            block_number,
            block_timestamp,
            spending_transaction_hash,
            spending_output_index
        FROM inputs 
        WHERE toYYYYMM(block_timestamp) = {partition}
    ) AS i
    INNER JOIN outputs AS o
        ON i.spending_transaction_hash = o.transaction_hash
    AND i.spending_output_index = o.output_index
    """
    client.command(insert_spent_sql)
    print(f"-- Populating outputs by inputs for partition {partition}...")


def scan_for_transaction_holes(client, from_partition: int = None):
    """
    Scans all partitions up to the first block hole, checking for transaction holes.
    Stops when the first transaction hole is found.
    """
    block_hole_partition = find_missing_block_partition(client)
    all_partitions = get_block_partitions(client)

    if block_hole_partition:
        print(f"🚧 First block hole found in partition {block_hole_partition}")
        partitions_to_check = [p for p in all_partitions if p <= block_hole_partition]
    else:
        print("✅ No block holes found — checking all partitions.")
        partitions_to_check = all_partitions

    # Apply from_partition filter
    if from_partition:
        partitions_to_check = [p for p in partitions_to_check if p >= from_partition]
        print(f"🔍 Scanning partitions from {from_partition} onwards...")

    for partition in partitions_to_check:
        if check_partition_for_holes(partition, client):
            print(f"⛔ Stopping: transaction hole found in partition {partition}")
            print(f"🚧 First block hole found in partition {block_hole_partition}")
            return

    print("✅ All checked partitions are transactionally consistent.")

def scan_and_update(client, from_partition: int = None):
    """
    Scans partitions for transaction holes and updates inputs and outputs tables.
    """
    block_hole_partition = find_missing_block_partition(client)
    all_partitions = get_block_partitions(client)

    if block_hole_partition:
        print(f"🚧 First block hole found in partition {block_hole_partition}")
        partitions_to_check = [p for p in all_partitions if p <= block_hole_partition]
    else:
        print("✅ No block holes found — checking all partitions.")
        partitions_to_check = all_partitions

    # Apply from_partition filter
    if from_partition:
        partitions_to_check = [p for p in partitions_to_check if p >= from_partition]
        print(f"🔍 Scanning partitions from {from_partition} onwards...")

    for partition in partitions_to_check:
        # final_transactions_fat(partition, client)

        if check_partition_for_holes(partition, client):
            print(f"⛔ Stopping: transaction hole found in partition {partition}")
            print(f"🚧 First block hole found in partition {block_hole_partition}")
            return

        populate_inputs(partition, client)
        populate_outputs(partition, client)
        populate_outputs_by_inputs(partition, client)
        print(f"✅ Partition {partition} is transactionally consistent and updated.")

    print("✅ All checked partitions are transactionally consistent and updated.")


#scan_for_transaction_holes(client)

def main():
    parser = argparse.ArgumentParser(description="Scan ClickHouse partitions for Bitcoin transaction holes.")
    parser.add_argument("--from_partition", type=int, help="Start scanning from this partition (inclusive), e.g., 201304")
    args = parser.parse_args()

    scan_and_update(client, from_partition=args.from_partition)
    # scan_for_transaction_holes(client, from_partition=args.from_partition)

if __name__ == "__main__":
    main()
