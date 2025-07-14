from clickhouse_connect import get_client
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

# SQL to get table sizes
query = """
SELECT 
    database, 
    table, 
    formatReadableSize(SUM(bytes)) AS total_size
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY total_size DESC
"""

# Execute and fetch results
result = client.query(query)

# Print nicely
print(f"{'Database':<20} {'Table':<30} {'Size':>15}")
print('-' * 70)
for row in result.result_rows:
    db, table, size = row
    print(f"{db:<20} {table:<30} {size:>15}")

query2 = """
SELECT 
    database, 
    formatReadableSize(SUM(bytes)) AS total_size 
FROM system.parts 
WHERE active 
GROUP BY database 
ORDER BY total_size DESC
"""

result2 = client.query(query2)
print('-' * 70)
for row in result2.result_rows:
    db, size = row
    print(f"{db:<20} {'Total Size':<30} {size:>15}") 