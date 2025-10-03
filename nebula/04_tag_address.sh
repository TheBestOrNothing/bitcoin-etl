# 04_tag_address.sh
set -euo pipefail
source ./env.sh

YM="${1:?Usage: $0 <YYYYMM>}"
CONF=./04_address_${YM}.conf

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
      name = address
      type = { source = jdbc, sink = client }
      driver = "com.clickhouse.jdbc.ClickHouseDriver"
      url = "$CK_JDBC"
      user = "$CK_USER"
      password = "$CK_PASS"
      numPartition = 32
      sentence = """
        SELECT DISTINCT
          toString(addr) AS vid,
          toString(addr) AS address
        FROM (
          SELECT arrayJoin(addresses) AS addr
          FROM outputs
          WHERE toYYYYMM(block_timestamp) = '$YM'
            AND length(toString(addr)) > 0
        )
      """
      fields        = [address]
      nebula.fields = [address]
      vertex    = { field = vid }
      batch     = $V_BATCH
      partition = $V_PART
    }
  ]
}
EOF

run_exchange "$CONF"

