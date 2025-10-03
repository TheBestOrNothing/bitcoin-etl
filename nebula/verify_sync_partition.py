from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
from clickhouse_connect import get_client
import argparse
from typing import Dict, Tuple, List

# ClickHouse configuration
CLICKHOUSE_CONFIG = {
    'host': '192.168.2.65',
    'port': 8123,
    'username': 'default',
    'password': 'password',
    'database': 'bitcoin'
}

# Nebula configuration
NEBULA_CONFIG = {
    'graphd_hosts': [('192.168.2.65', 9669)],
    'username': 'root',
    'password': 'nebula',
    'space': 'bitcoin'
}

class SyncVerifier:
    def __init__(self):
        # Initialize ClickHouse client
        self.ch_client = get_client(**CLICKHOUSE_CONFIG)
        
        # Initialize Nebula client with custom timeout settings
        config = Config()
        config.max_connection_pool_size = 10
        config.timeout = 3600000  # 3000 seconds
        config.idle_time = 0
        config.retry_times = 3
        
        self.nebula_connection_pool = ConnectionPool()
        self.nebula_connection_pool.init(NEBULA_CONFIG['graphd_hosts'], config)

    def get_clickhouse_counts(self, partition: str) -> Dict[str, int]:
        """Get counts from ClickHouse for the specified partition"""
        counts = {}
        
        # Count blocks
        block_query = f"""
            SELECT COUNT(*) 
            FROM blocks 
            WHERE toYYYYMM(timestamp) = {partition}
        """
        counts['blocks'] = self.ch_client.query(block_query).result_rows[0][0]
        print(f"Blocks count for partition {partition}: {counts['blocks']}")
        
        # Count transactions
        tx_query = f"""
            SELECT COUNT(*) 
            FROM transactions 
            WHERE toYYYYMM(block_timestamp) = {partition}
        """
        counts['transactions'] = self.ch_client.query(tx_query).result_rows[0][0]
        print(f"Transactions count for partition {partition}: {counts['transactions']}")
        
        # Count outputs
        output_query = f"""
            SELECT COUNT(*) 
            FROM outputs 
            WHERE toYYYYMM(block_timestamp) = {partition}
        """
        counts['outputs'] = self.ch_client.query(output_query).result_rows[0][0]
        print(f"Outputs count for partition {partition}: {counts['outputs']}")
        
        # Count unique addresses - Modified query to handle Array type
        address_query = f"""
            SELECT COUNT(DISTINCT arrayJoin(addresses)) 
            FROM outputs 
            WHERE toYYYYMM(block_timestamp) = {partition}
            AND notEmpty(addresses)
        """
        counts['addresses'] = self.ch_client.query(address_query).result_rows[0][0]
        print(f"Addresses count for partition {partition}: {counts['addresses']}")

        # Count belongs_to edges (transactions to blocks)
        counts['belongs_to'] = counts['transactions']  # Each transaction belongs to one block
        print(f"Belongs_to edges count for partition {partition}: {counts['belongs_to']}")

        spent_by_query = f"""
            SELECT count()
            FROM outputs
            WHERE (toYYYYMM(block_timestamp) = {partition}) AND (toUInt64(revision) > 0)
        """
        counts['spent_by'] = self.ch_client.query(spent_by_query).result_rows[0][0]
        print(f"Spent_by edges count for partition {partition}: {counts['spent_by']}")

        counts['lock_to'] = counts['outputs']  # all outputs should be locked to a transaction
        print(f"Lock_to edges count for partition {partition}: {counts['lock_to']}")

        pay_to_query = f"""
            SELECT count()
            FROM (
                SELECT transaction_hash, output_index, arrayJoin(addresses) AS addr
                FROM outputs
                WHERE toYYYYMM(block_timestamp) = {partition}
                    AND length(toString(addr)) > 0
            )
        """
        counts['pay_to'] = self.ch_client.query(pay_to_query).result_rows[0][0]
        print(f"Pay_to edges count for partition {partition}: {counts['pay_to']}")

        # Count coinbase transactions and edges
        coinbase_query = f"""
            SELECT count()
            FROM transactions
            WHERE toYYYYMM(block_timestamp) = {partition}
                AND toBool(is_coinbase) = true
        """
        counts['coinbase'] = self.ch_client.query(coinbase_query).result_rows[0][0]
        print(f"Coinbase transactions count for partition {partition}: {counts['coinbase']}")

        counts['chain_to'] = counts['blocks'] - 1  # All but genesis block
        print(f"Chain_to edges count for partition {partition}: {counts['chain_to']}")

        return counts

    def get_nebula_counts(self, partition: str) -> Dict[str, int]:
        """Get counts from Nebula for the specified partition"""
        counts = {}
        
        with self.nebula_connection_pool.session_context(
            NEBULA_CONFIG['username'],
            NEBULA_CONFIG['password']
        ) as session:
            session.execute(f'USE {NEBULA_CONFIG["space"]}')
            
            # Count vertices
            vertex_queries = {
                'blocks': f'MATCH (v:block) WHERE (v.block.block_timestamp).year * 100 + (v.block.block_timestamp).month == {partition} RETURN COUNT(v)',
                'transactions': f'MATCH (v:transaction)-[:belongs_to]->(b:block) WHERE (b.block.block_timestamp).year * 100 + (b.block.block_timestamp).month == {partition} RETURN COUNT(v)',
                'outputs': f'MATCH (v:output)<-[:lock_to]-(t:transaction)-[:belongs_to]->(b:block) WHERE (b.block.block_timestamp).year * 100 + (b.block.block_timestamp).month == {partition} RETURN COUNT(v)',
                'addresses': f'MATCH (v:address)<-[:pay_to]-(o:output)<-[:lock_to]-(t:transaction)-[:belongs_to]->(b:block) WHERE (b.block.block_timestamp).year * 100 + (b.block.block_timestamp).month == {partition} RETURN COUNT(DISTINCT v)'
            }
            
            for tag, query in vertex_queries.items():
                result = session.execute(query)
                # Access the count value from the result
                if result.is_succeeded() and result.rows():
                    #val = result.rows()[0].values[0]
                    #print(val)
                    #print(type(val))
                    #print(dir(val))
                    #pirnt(val.get_iVal())
                    counts[tag] = result.rows()[0].values[0].get_iVal()
                    print(f"{tag.capitalize()} count in Nebula for partition {partition}: {counts[tag]}")
                else:
                    counts[tag] = 0
            
            # Count edges
            edge_queries = {
                'belongs_to': f'MATCH ()-[e:belongs_to]->() RETURN COUNT(e)',
                'spent_by': f'MATCH (o:output)-[e:spent_by]->(t:transaction)-[:belongs_to]->(b:block) RETURN COUNT(e)',
                'lock_to': f' MATCH (t:transaction)-[e:lock_to]->(o:output) RETURN COUNT(e)',
                'pay_to': f'MATCH (o:output)-[e:pay_to]->(a:address) RETURN COUNT(e)',
                'coinbase': f'MATCH (b:block)-[e:coinbase]->(t:transaction) RETURN COUNT(e)',
                'chain_to': f'MATCH (child:block)-[e:chain_to]->(parent:block) RETURN COUNT(e)'
            }
            
            # Additional edge queries with partition filtering
            edge_queries2 = {
                'belongs_to': f'''MATCH (t:transaction)-[e:belongs_to]->(b:block) 
                                 WHERE (b.block.block_timestamp).year * 100 + (b.block.block_timestamp).month == {partition} 
                                 RETURN COUNT(e)''',
                
                'spent_by': f'''MATCH (o:output)-[e:spent_by]->(t:transaction)-[:belongs_to]->(b:block) 
                                WHERE (b.block.block_timestamp).year * 100 + (b.block.block_timestamp).month == {partition} 
                                RETURN COUNT(e)''',
                
                'lock_to': f'''MATCH (t:transaction)-[e:lock_to]->(o:output) 
                               WITH t, e, o MATCH (t)-[:belongs_to]->(b:block)
                               WHERE (b.block.block_timestamp).year * 100 + (b.block.block_timestamp).month == {partition} 
                               RETURN COUNT(e)''',
                
                'pay_to': f'''MATCH (o:output)-[e:pay_to]->(a:address) 
                              WITH o, e, a MATCH (t:transaction)-[:lock_to]->(o)-[:belongs_to]->(b:block)
                              WHERE (b.block.block_timestamp).year * 100 + (b.block.block_timestamp).month == {partition} 
                              RETURN COUNT(e)''',
                
                'coinbase': f'''MATCH (b:block)-[e:coinbase]->(t:transaction) 
                                WHERE (b.block.block_timestamp).year * 100 + (b.block.block_timestamp).month == {partition} 
                                RETURN COUNT(e)''',
                
                'chain_to': f'''MATCH (child:block)-[e:chain_to]->(parent:block) 
                                WHERE (child.block.block_timestamp).year * 100 + (child.block.block_timestamp).month == {partition} 
                                RETURN COUNT(e)'''
            }
            
            for edge, query in edge_queries2.items():
                result = session.execute(query)
                # Access the count value from the result
                if result.is_succeeded() and result.rows():
                    counts[edge] = result.rows()[0].values[0].get_iVal()
                    print(f"{edge} edges count in Nebula for partition {partition}: {counts[edge]}")
                else:
                    counts[edge] = 0
    
        return counts

    def verify_partition(self, partition: str) -> Tuple[bool, Dict]:
        """Verify data consistency for a given partition"""
        ch_counts = self.get_clickhouse_counts(partition)
        nebula_counts = self.get_nebula_counts(partition)
        
        discrepancies = {}
        is_consistent = True
        
        # Compare counts
        for entity in ch_counts:
            if ch_counts[entity] != nebula_counts[entity]:
                is_consistent = False
                discrepancies[entity] = {
                    'clickhouse': ch_counts[entity],
                    'nebula': nebula_counts[entity],
                    'difference': abs(ch_counts[entity] - nebula_counts[entity])
                }
        
        return is_consistent, discrepancies
    
    def list_clickhouse_counts(self, partition: str):
        counts = self.get_clickhouse_counts(partition)
        print(f"ClickHouse counts for partition {partition}:")
        for entity, count in counts.items():
            print(f"  {entity}: {count}")
    
    def get_partitions(self) -> List[str]:
        """Get all available partitions from ClickHouse"""
        partition_query = """
            SELECT DISTINCT toString(toYYYYMM(timestamp)) as partition
            FROM blocks
            ORDER BY partition DESC
        """
        result = self.ch_client.query(partition_query)
        return [row[0] for row in result.result_rows]

def main():
    parser = argparse.ArgumentParser(description='Verify data sync between ClickHouse and Nebula Graph')
    parser.add_argument('--partition', type=str, help='Specific partition to verify (YYYYMM format)')
    parser.add_argument('--all', action='store_true', help='Verify all partitions')
    args = parser.parse_args()
    
    verifier = SyncVerifier()
    
    if args.all:
        partitions = verifier.get_partitions()
        print(f"Found {len(partitions)} partitions: {partitions}")
        
        # For shell script usage
        print("\nFor shell script:")
        print(f"PARTITIONS=({' '.join(partitions)})")
        
        for partition in partitions:
            print(f"\n=== Verifying partition {partition} ===")
            is_consistent, discrepancies = verifier.verify_partition(partition)
            if is_consistent:
                print(f"✅ Partition {partition} is consistent")
            else:
                print(f"❌ Partition {partition} has discrepancies:")
                for entity, details in discrepancies.items():
                    print(f"\n{entity}:")
                    print(f"  ClickHouse count: {details['clickhouse']}")
                    print(f"  Nebula count: {details['nebula']}")
                    print(f"  Difference: {details['difference']}")
    elif args.partition:
        is_consistent, discrepancies = verifier.verify_partition(args.partition)
        if is_consistent:
            print(f"✅ Partition {args.partition} is consistent")
        else:
            print(f"❌ Partition {args.partition} has discrepancies:")
            for entity, details in discrepancies.items():
                print(f"\n{entity}:")
                print(f"  ClickHouse count: {details['clickhouse']}")
                print(f"  Nebula count: {details['nebula']}")
                print(f"  Difference: {details['difference']}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()