from clickhouse_connect import get_client
from datetime import datetime
from dateutil.relativedelta import relativedelta

from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc

from blockchainetl.thread_local_proxy import ThreadLocalProxy
from bitcoinetl.streaming.streaming_utils import get_item_exporter
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter
from blockchainetl.streaming.streamer import Streamer


from typing import Sequence, List

# === CONFIGURATION ===
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'password'
DATABASE = 'bitcoin'

END_PARTITION = 201601 # e.g. 201501 for January 2015

# === INIT CLIENT ===
client = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE,
)

def fix_block_hole(missing_blocks):
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
    for block_number in missing_blocks:
        # Export the specific block
        streamer_adapter.export_all(start_block=block_number, end_block=block_number)
        print(f"Fixed Block: {block_number}")

    streamer_adapter.close()


def missing_blocks_by_partition(
    client,          # clickhouse-connect or clickhouse-driver client
    partition: int,  # e.g. 202504
) -> List[int]:
    """
    Find every block number that is **absent** from the blocks_fat table,
    where the sequence length is determined by the highest block number
    present in the specified YYYYMM partition.

    Parameters
    ----------
    client : ClickHouse client
        An already-constructed clickhouse_connect.Client or clickhouse_driver.Client.
    partition : int
        The target partition as an integer in YYYYMM form (e.g. 202504).

    Returns
    -------
    list[int]
        Sorted list of block numbers that are missing.
    """

    query = """
    WITH seq AS (
        SELECT number
        FROM numbers(
            toUInt64(
                ifNull(
                    (SELECT max(number)
                     FROM blocks_fat
                     WHERE toYYYYMM(timestamp_month) = %(partition)s),
                0) + 1
            )
        )
    )
    SELECT seq.number AS missing_block_number
    FROM seq
    LEFT ANTI JOIN blocks_fat AS b ON seq.number = b.number
    ORDER BY missing_block_number
    """

    # clickhouse-connect → .result_rows ; clickhouse-driver → .execute & fetchall
    rows: Sequence[Sequence[int]] = client.query(
        query,
        parameters={"partition": partition}
    ).result_rows

    return [row[0] for row in rows]


missing_blocks = missing_blocks_by_partition(client, END_PARTITION)
if missing_blocks:
    print(f"Missing blocks in partition {END_PARTITION}: {len(missing_blocks)}")
    print(f"First Missing blocks : {missing_blocks[0]}")
    # for block_number in missing_blocks:
    #   print(f"  Block Number: {block_number}")
    
    # Fix the hole by exporting the missing blocks
    fix_block_hole(missing_blocks)

print(f"No Missing blocks before partition {END_PARTITION}")
