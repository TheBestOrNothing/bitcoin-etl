# env.sh
set -euo pipefail

# --- Spark / Exchange ---
export SPARK_HOME=/opt/spark
export EXCHANGE_JAR=/opt/nebula-exchange/nebula-exchange_spark_3.4-3.x.y.jar   # <-- set your real jar
export SPARK_MASTER="local[8]"  # or yarn / k8s

# --- Nebula ---
export NEBULA_GRAPH_ADDR="192.168.2.65:9669"
export NEBULA_META_ADDR="192.168.2.65:32768"   # meta leader port
export NEBULA_USER="root"
export NEBULA_PSWD="nebula"
export NEBULA_SPACE="bitcoin"

# --- ClickHouse ---
export CK_JDBC="jdbc:clickhouse://192.168.2.65:8123/bitcoin"
export CK_USER="default"
export CK_PASS="password"

# --- Partition to sync ---
export YM="202506"

# --- batching ---
export V_BATCH=1024
export V_PART=64
export E_BATCH=2048
export E_PART=64

# --- misc ---
export ERR_DIR="/tmp/nebula-exchange-errors"
mkdir -p "$ERR_DIR"

run_exchange() {
  local conf_file="$1"
  "$SPARK_HOME/bin/spark-submit" \
    --class com.vesoft.nebula.exchange.Exchange \
    --master "$SPARK_MASTER" \
    --conf spark.app.name=btc-clickhouse-to-nebula \
    --conf spark.driver.cores=1 \
    --conf spark.driver.maxResultSize=32g \
    --conf spark.cores.max=32 \
    "$EXCHANGE_JAR" -c "$conf_file"
}

