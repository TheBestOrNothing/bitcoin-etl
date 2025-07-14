import argparse
from clickhouse_connect import get_client

from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc

from blockchainetl.thread_local_proxy import ThreadLocalProxy
from bitcoinetl.streaming.streaming_utils import get_item_exporter
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter

from datetime import datetime
import time
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

def max_number_in_blocks(client):
    query = f"""
        SELECT max(number)
        FROM bitcoin.blocks
    """
    results = client.query(query)

    if results and results[0][0] is not None:
        return int(results[0][0])
    else:
        return 0  # or raise an exception if preferred

def max_number_in_blocks_fat(client):
    query = f"""
        SELECT max(number)
        FROM bitcoin.blocks_fat
    """
    results = client.query(query)

    if results and results[0][0] is not None:
        return int(results[0][0])
    else:
        return 0  # or raise an exception if preferred

def is_missing_block_in_fat(client, number):
    query = f"""
        SELECT count(*) > 0
        FROM bitcoin.blocks_fat 
        WHERE number = {number}
    """
    result = client.query(query)
    return result.result_rows[0][0] == 0

def patch_missing_block(missing_block):
    """Fixes a hole in the blockchain by exporting a specific block."""

    provider_uri = 'http://bitcoin:passw0rd@localhost:8332'
    output = 'kafka/localhost:9092'
    chain = Chain.BITCOIN
    batch_size = 1
    enrich = True
    max_workers = 1

    streamer_adapter = BtcStreamerAdapter(
        bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
        item_exporter=get_item_exporter(output),
        chain=chain,
        batch_size=batch_size,
        enable_enrich=enrich,
        max_workers=max_workers
    )
    
    streamer_adapter.open()
    # Export the specific block
    streamer_adapter.export_all(start_block=missing_block, end_block=missing_block)
    print(f"Patching Block: {missing_block}")
    streamer_adapter.close()

def is_missing_tx_in_fat(client, block_number, partition):
    """Check if there are transactions in the block that are missing in the fat table."""
    query = f"""
    WITH flattened AS (
        SELECT
            hash AS block_hash,
            tx_hash,
            number
        FROM bitcoin.blocks_fat
        ARRAY JOIN transactions AS tx_hash
        WHERE toYYYYMM(block_timestamp) = {partition}
            AND number = {block_number}
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
            AND number = {block_number}
    ) AS txs
    ON flattened.tx_hash = txs.hash
    """
    result = client.query(query)
    return len(result.result_rows) > 0

def patch_missing_tx_in_fat(client, block_number, partition):
    patch_missing_block(block_number)

def finalize_blocks_fat(client, partition):
    """Finalize blocks in the fat table for a specific partition."""
    query = f"OPTIMIZE TABLE blocks_fat PARTITION '{partition}' FINAL"
    client.command(query)
    print(f"Finalized blocks in fat table for partition {partition}.")

def finalize_transactions_fat(client, partition):
    """Finalize transactions in the fat table for a specific partition."""
    query = f"OPTIMIZE TABLE transactions_fat PARTITION '{partition}' FINAL"
    client.command(query)
    print(f"Finalized transactions in fat table for partition {partition}.")

def copy_block_from_fat(client, block_number, partition):
    """Copy a block from the fat table to the main table."""
    query = f"""
    INSERT INTO bitcoin.blocks
    SELECT *
    FROM bitcoin.blocks_fat
    WHERE number = {block_number}
      AND toYYYYMM(block_timestamp) = {partition}
    """
    client.command(query)
    print(f"Copied block {block_number} from fat to main table for partition {partition}.")

def copy_transactions_from_fat(client, block_number, partition):
    """Copy transactions from the fat table to the main table."""
    query = f"""
    INSERT INTO bitcoin.transactions
    SELECT *
    FROM bitcoin.transactions_fat
    WHERE block_number = {block_number}
      AND toYYYYMM(block_timestamp) = {partition}
    """
    client.command(query)
    print(f"Copied transactions for block {block_number} from fat to main table for partition {partition}.")

def populate_inputs(client, block_number, partition):
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
    AND t.block_number = {block_number}
    """
    client.command(insert_inputs_sql)
    print(f"-- Populating inputs for partition {partition}...")


def populate_outputs(client, block_number, partition):

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
    AND t.block_number = {block_number}
    """
    client.command(insert_outputs_sql)
    print(f"-- Populating outputs for partition {partition}...")

def populate_outputs_by_inputs(client, block_number, partition):
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
    AND i.block_number = {block_number}
    """
    client.command(insert_spent_sql)
    print(f"-- Populating outputs by inputs for partition {partition}...")


def populate_inputs_by_outputs(client, block_number, partition):
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
    AND i.block_number = {block_number}
    """
    client.command(insert_spent_sql)
    print(f"-- Populating inputs by outputs for partition {partition}...")

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

def has_duplicate_tx(client, block_number, partition):
    query = f"""
        SELECT count(*) > 0
        FROM (
            SELECT hash, count(*) AS c
            FROM transactions_fat
            WHERE toYYYYMM(block_timestamp) = {partition}
            AND block_number = {block_number}
            GROUP BY hash
            HAVING c > 1
        )
    """
    result = client.query(query)
    return result.result_rows[0][0] == 1

def has_duplicate_blocks(client, block_number, partition):
    query = f"""
        SELECT count(*) > 0
        FROM (
            SELECT hash, count(*) AS c
            FROM blocks_fat
            WHERE toYYYYMM(timestamp_month) = {partition}
            AND number = {block_number}
            GROUP BY hash
            HAVING c > 1
        )
    """
    result = client.query(query)
    return result.result_rows[0][0] == 1

def get_block_partition(client, block_number):
    query = f"""
        SELECT timestamp
        From bitcoin.blocks_fat
        WHERE number = {block_number}
    """
    result = client.query(query)
    timestamp_str = result.result_rows[0][0]
    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.strftime("%Y%m"))

def main():
    parser = argparse.ArgumentParser(description="Deduplicate partitions in blocks from a start partition onward")
    parser.add_argument("--start-partition", default=DEFAULT_START_PARTITION, help="Start partition (YYYYMM)")
    args = parser.parse_args()

    while True:
        try:
            block = max_number_in_blocks(client) + 1

            if is_missing_block_in_fat(client, block):
                print(f"⚠️  Missing block {block} in fat table. Patching...")
                time.sleep(180)
                continue

            if block == max_number_in_blocks_fat(client):
                print("New blocks is processing. Sleeping for 3 minutes...")
                time.sleep(180)
                continue

            partition = get_block_partition(client, block)
            if is_missing_tx_in_fat(client, block, partition):
                print(f"⚠️  Missing transactions in block {block} in fat table. Patching...")
                patch_missing_tx_in_fat(client, block, partition)
                continue

            if has_duplicate_blocks(client, block, partition):
                print(f"⚠️  Duplicate blocks in fat table for block {block}. Copying to main table...")
                finalize_blocks_fat(client, partition)
                continue

            if has_duplicate_tx(client, block, partition):
                print(f"⚠️  Duplicate transactions in block {block} in fat table. Patching...")
                finalize_transactions_fat(client, partition)
                continue

            copy_block_from_fat(client, block, partition)
            copy_transactions_from_fat(client, block, partition)
            populate_inputs(client, block, partition)
            populate_outputs(client, block, partition)
            populate_outputs_by_inputs(client, block, partition)
            populate_inputs_by_outputs(client, block, partition)
            print(f"✅ Inputs and Outputs data sync done for block {block} in partition {partition}.")

        except Exception as e:
            print(e)
            print("⏳ Sleeping for 3 minutes before retrying...")
            time.sleep(180)
            continue


if __name__ == "__main__":
    main()
