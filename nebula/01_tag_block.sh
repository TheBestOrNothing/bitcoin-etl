# 01_tag_block.sh
set -euo pipefail
source ./env.sh

YM="${1:?Usage: $0 <YYYYMM>}"
CONF=./01_block_${YM}.conf

cat > "$CONF" <<EOF
{
  spark = {
    app = { name = "btc-clickhouse-to-nebula" }
    driver = { cores = 1, maxResultSize = "32G" }
    cores = { max = 32 }
  }

  nebula = {
    address = {
      graph = ["$NEBULA_GRAPH_ADDR"]
      meta  = ["$NEBULA_META_ADDR"]
    }
    user = "$NEBULA_USER"
    pswd = "$NEBULA_PSWD"
    space = "$NEBULA_SPACE"
    connection = { timeout = 30000 }
    execution  = { retry = 3 }
    error      = { max = 32, output = "$ERR_DIR" }
    rate       = { limit = 1024, timeout = 1000 }
  }

  ck = {
    driver       = "com.clickhouse.jdbc.ClickHouseDriver"
    url          = "$CK_JDBC"
    user         = "$CK_USER"
    password     = "$CK_PASS"
    numPartition = 32
  }

  tags = [
    {
      name = block
      type = { source = jdbc, sink = client }
      driver       = "com.clickhouse.jdbc.ClickHouseDriver"
      url          = "$CK_JDBC"
      user         = "$CK_USER"
      password     = "$CK_PASS"
      numPartition = 32
      sentence = """
        SELECT
          toString(hash)                                  AS vid,
          toInt64(number)                                 AS block_number,
	  formatDateTime(timestamp, '%Y-%m-%dT%H:%i:%s')  AS block_timestamp,
          toString(previous_block_hash)                   AS previous_block_hash
        FROM blocks
        WHERE toYYYYMM(timestamp) = '$YM'
      """
      fields        = [block_number, block_timestamp, previous_block_hash]
      nebula.fields = [block_number, block_timestamp, previous_block_hash]
      vertex    = { field = vid }
      batch     = $V_BATCH
      partition = $V_PART
    }
  ]
}
EOF

# run it
run_exchange "$CONF"

