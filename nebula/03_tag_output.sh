# 03_tag_output.sh
set -euo pipefail
source ./env.sh

YM="${1:?Usage: $0 <YYYYMM>}"
CONF=./03_output_${YM}.conf

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

  tags = [
    {
      name = output
      type = { source = jdbc, sink = client }
      driver = "com.clickhouse.jdbc.ClickHouseDriver"
      url = "$CK_JDBC"
      user = "$CK_USER"
      password = "$CK_PASS"
      numPartition = 32
      sentence = """
        SELECT
          toString(concat(transaction_hash, ':', toString(output_index))) AS vid,
          toString(transaction_hash)                                      AS transaction_hash,
          toInt64(output_index)                                           AS output_index,
          toString(type)                                                  AS output_type,
          toFloat64(value)                                                AS output_value
        FROM outputs
        WHERE toYYYYMM(block_timestamp) = '$YM'
      """
      fields        = [transaction_hash, output_index, output_type, output_value]
      nebula.fields = [transaction_hash, output_index, output_type, output_value]
      vertex    = { field = vid }
      batch     = $V_BATCH
      partition = $V_PART
    }
  ]
}
EOF

run_exchange "$CONF"

