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


# === INIT CLIENT ===
client = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE,
)

def missing_blocks_all_partitions(client) -> List[int]:
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
    return [row[0] for row in rows]

missing_blocks = missing_blocks_all_partitions(client)

if missing_blocks:
    print(f"Found {len(missing_blocks)} missing blocks across all partitions.")
    print("First Missing Block Numbers:", missing_blocks[0])
else:
    print("No missing blocks found across all partitions.")