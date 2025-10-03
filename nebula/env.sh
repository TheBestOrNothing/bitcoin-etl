# env.sh
set -euo pipefail

# --- Spark / Exchange ---
export SPARK_HOME=/opt/spark
export JDBC_JAR=/opt/nebula-exchange/clickhouse-jdbc-0.9.2-all.jar
export EXCHANGE_JAR=/opt/nebula-exchange/nebula-exchange_spark_3.0-3.8.0.jar
export SPARK_MASTER="local"

# --- Nebula ---
export NEBULA_GRAPH_ADDR="192.168.2.65:9669"
export NEBULA_META_ADDR="192.168.2.65:32768"
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
export ERR_DIR="./nebula-exchange-errors"
mkdir -p "$ERR_DIR"

run_exchange() {
  local conf_file="$1"
  "${SPARK_HOME}/bin/spark-submit" \
    --master "$SPARK_MASTER" \
    --class com.vesoft.nebula.exchange.Exchange \
    --jars "$JDBC_JAR" \
    "$EXCHANGE_JAR" -c "$conf_file"
}

