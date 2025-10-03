# 06_edge_lock_to.sh
set -euo pipefail
source ./env.sh

YM="${1:?Usage: $0 <YYYYMM>}"
CONF=./06_edge_lock_to_${YM}.conf

cat > "$CONF" <<EOF
{
  spark = {
    app = { name = "btc-clickhouse-to-nebula" }
    driver = { cores = 1, maxResultSize = "32G" }
    cores = { max = 32 }
  }

  nebula = {
    address = { graph = ["$NEBULA_GRAPH_ADDR"], meta = ["$NEBULA_META_ADDR"] }
    user = "$NEBULA_USER"
    pswd = "$NEBULA_PSWD"
    space = "$NEBULA_SPACE"
    connection = { timeout = 30000 }
    execution  = { retry = 3 }
    error      = { max = 32, output = "$ERR_DIR" }
    rate       = { limit = 1024, timeout = 1000 }
  }

  ck = {
    driver = "com.clickhouse.jdbc.ClickHouseDriver"
    url = "$CK_JDBC"
    user = "$CK_USER"
    password = "$CK_PASS"
    numPartition = 32
  }

  edges = [
    {
      name = lock_to
      type = { source = jdbc, sink = client }
      driver = "com.clickhouse.jdbc.ClickHouseDriver"
      url = "$CK_JDBC"
      user = "$CK_USER"
      password = "$CK_PASS"
      numPartition = 32
      sentence = """
        SELECT
          toString(transaction_hash)                                        AS src,
          toString(concat(transaction_hash, ':', toString(output_index)))   AS dst,
          toInt64(output_index)                                             AS output_index
        FROM outputs
        WHERE toYYYYMM(block_timestamp) = '$YM'
      """
      fields        = [output_index]
      nebula.fields = [output_index]
      source    = { field = src }
      target    = { field = dst }
      batch     = $E_BATCH
      partition = $E_PART
    }
  ]
}
EOF

run_exchange "$CONF"

