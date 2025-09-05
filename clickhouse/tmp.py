import clickhouse_connect
from clickhouse_connect import get_client

# === CONFIGURATION ===
CLICKHOUSE_HOST = 'localhost'
#CLICKHOUSE_HOST = '192.168.2.65'
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

print(client.command('SELECT version()'))

here is input_output json schema from kafka,  please convert the python to insert vertex and edge to janusgraph

1. create output vertex with input_output.transaction_hash, input_output.output_index, input_output. value
2. create address vertex with input_output.address, input_output.value
3. create transaction vertex with input_output.transaction_hash  input_output.block_hash  input_output.block_number  input_output.block_timestamp input_output.is_coinbase
3. create spentIn edge from output → transaction, where input_output.spent_transaction_hash = transaction. hash

input_output
(
    transaction_hash String,
    output_index UInt64,
    block_hash String,
    block_number UInt64,
    block_timestamp DateTime,
    spent_transaction_hash String,
    spent_input_index UInt64,
    spent_block_hash String,
    spent_block_number UInt64,
    spent_block_timestamp DateTime,
    addresses Array(String),
    value Float64,
    is_coinbase BOOL,
    revision UInt64
)

as to Create spentIn edge: output → transaction, 
1. find vertex output, where ouput.transaction_hash = input_output.transaction_hash and output.output_index = input_output.output_index
2. find vertex transaction where transaction.hash = input_output.spent_transaction_hash
3. create spentIn edge: output → transaction

idxOutputKey = mgmt.buildIndex('output_by_txhash_vout', Vertex.class).addKey(transactionHash).addKey(outputIndex).indexOnly(vOutput).unique().buildCompositeIndex()

idxTxKey = mgmt.buildIndex('tx_by_hash', Vertex.class).addKey(transactionHash).indexOnly(vTransaction).unique().buildCompositeIndex()

idxBlockByHash = mgmt.buildIndex('block_by_hash', Vertex.class).addKey(blockHash).indexOnly(vBlock).unique().buildCompositeIndex()

idxAddressKey = mgmt.buildIndex('address_by_addr', Vertex.class).addKey(addressProp).indexOnly(vAddress).unique().buildCompositeIndex()

for (name in ['output_by_txhash_vout','tx_by_hash','block_by_hash','address_by_addr','block_by_height','tx_by_block_hash','output_by_tx_hash']) { mgmt.awaitGraphIndexStatus(graph, name).status(SchemaStatus.REGISTERED).call()}