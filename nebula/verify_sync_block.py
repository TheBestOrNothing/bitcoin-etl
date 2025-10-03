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

class BlockSyncVerifier:
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

    def get_clickhouse_block_counts(self, block_number: int) -> Dict[str, int]:
        """Get counts from ClickHouse for a specific block"""
        counts = {}
        
        # Count block
        block_query = f"SELECT COUNT(*) FROM blocks WHERE number = {block_number}"
        counts['block'] = self.ch_client.query(block_query).result_rows[0][0]
        
        # Count transactions
        tx_query = f"SELECT COUNT(*) FROM transactions WHERE block_number = {block_number}"
        counts['transactions'] = self.ch_client.query(tx_query).result_rows[0][0]
        
        # Count outputs
        output_query = f"SELECT COUNT(*) FROM outputs WHERE block_number = {block_number}"
        counts['outputs'] = self.ch_client.query(output_query).result_rows[0][0]
        
        # Count unique addresses
        address_query = f"""
            SELECT COUNT(DISTINCT arrayJoin(addresses)) 
            FROM outputs 
            WHERE block_number = {block_number}
            AND notEmpty(addresses)
        """
        counts['addresses'] = self.ch_client.query(address_query).result_rows[0][0]

        # Count edges
        counts['belongs_to'] = counts['transactions']
        
        spent_by_query = f"""
            SELECT count()
            FROM outputs
            WHERE block_number = {block_number} AND (toUInt64(revision) > 0)
        """
        counts['spent_by'] = self.ch_client.query(spent_by_query).result_rows[0][0]
        
        counts['lock_to'] = counts['outputs']
        
        pay_to_query = f"""
            SELECT count()
            FROM (
                SELECT arrayJoin(addresses) AS addr
                FROM outputs
                WHERE block_number = {block_number}
                    AND length(toString(addr)) > 0
            )
        """
        counts['pay_to'] = self.ch_client.query(pay_to_query).result_rows[0][0]
        
        coinbase_query = f"""
            SELECT count()
            FROM transactions
            WHERE block_number = {block_number}
                AND toBool(is_coinbase) = true
        """
        counts['coinbase'] = self.ch_client.query(coinbase_query).result_rows[0][0]
        
        # Chain_to is always 1 for each block except genesis
        counts['chain_to'] = 1 if block_number > 0 else 0
        
        return counts

    def get_nebula_block_counts(self, block_number: int) -> Dict[str, int]:
        """Get counts from Nebula for a specific block"""
        counts = {}
        
        with self.nebula_connection_pool.session_context(
            NEBULA_CONFIG['username'],
            NEBULA_CONFIG['password']
        ) as session:
            session.execute(f'USE {NEBULA_CONFIG["space"]}')
            
            # Vertex queries
            vertex_queries = {
                'block': f'MATCH (v:block) WHERE v.block.block_number == {block_number} RETURN COUNT(v)',
                'transactions': f'MATCH (v:transaction)-[:belongs_to]->(b:block) WHERE b.block.block_number == {block_number} RETURN COUNT(v)',
                'outputs': f'MATCH (v:output)<-[:lock_to]-(t:transaction)-[:belongs_to]->(b:block) WHERE b.block.block_number == {block_number} RETURN COUNT(v)',
                'addresses': f'MATCH (v:address)<-[:pay_to]-(o:output)<-[:lock_to]-(t:transaction)-[:belongs_to]->(b:block) WHERE b.block.block_number == {block_number} RETURN COUNT(DISTINCT v)'
            }
            
            # Edge queries
            edge_queries = {
                'belongs_to': f'MATCH (t:transaction)-[e:belongs_to]->(b:block) WHERE b.block.block_number == {block_number} RETURN COUNT(e)',
                'spent_by': f'MATCH (o:output)-[e:spent_by]->(t:transaction)-[:belongs_to]->(b:block) WHERE b.block.block_number == {block_number} RETURN COUNT(e)',
                'lock_to': f'MATCH (t:transaction)-[e:lock_to]->(o:output) WITH t, e, o MATCH (t)-[:belongs_to]->(b:block) WHERE b.block.block_number == {block_number} RETURN COUNT(e)',
                'pay_to': f'MATCH (o:output)-[e:pay_to]->(a:address) WITH o, e, a MATCH (t:transaction)-[:lock_to]->(o)-[:belongs_to]->(b:block) WHERE b.block.block_number == {block_number} RETURN COUNT(e)',
                'coinbase': f'MATCH (b:block)-[e:coinbase]->(t:transaction) WHERE b.block.block_number == {block_number} RETURN COUNT(e)',
                'chain_to': f'MATCH (child:block)-[e:chain_to]->(parent:block) WHERE child.block.block_number == {block_number} RETURN COUNT(e)'
            }
            
            # Execute all queries
            for queries in [vertex_queries, edge_queries]:
                for entity, query in queries.items():
                    result = session.execute(query)
                    if result.is_succeeded() and result.rows():
                        counts[entity] = result.rows()[0].values[0].get_iVal()
                    else:
                        counts[entity] = 0
        
        return counts

    def verify_block(self, block_number: int) -> Tuple[bool, Dict]:
        """Verify data consistency for a specific block"""
        ch_counts = self.get_clickhouse_block_counts(block_number)
        nebula_counts = self.get_nebula_block_counts(block_number)
        
        discrepancies = {}
        is_consistent = True
        
        for entity in ch_counts:
            if ch_counts[entity] != nebula_counts[entity]:
                is_consistent = False
                discrepancies[entity] = {
                    'clickhouse': ch_counts[entity],
                    'nebula': nebula_counts[entity],
                    'difference': abs(ch_counts[entity] - nebula_counts[entity])
                }
        
        return is_consistent, discrepancies

    def get_block_range(self) -> Tuple[int, int]:
        """Get the available block range from ClickHouse"""
        query = "SELECT MIN(number), MAX(number) FROM blocks"
        result = self.ch_client.query(query)
        return result.result_rows[0][0], result.result_rows[0][1]

def main():
    parser = argparse.ArgumentParser(description='Verify data sync between ClickHouse and Nebula Graph')
    parser.add_argument('--block', type=int, help='Specific block number to verify')
    parser.add_argument('--start-block', type=int, help='Start block number for range verification')
    parser.add_argument('--end-block', type=int, help='End block number for range verification')
    args = parser.parse_args()
    
    verifier = BlockSyncVerifier()
    
    if args.block is not None:
        is_consistent, discrepancies = verifier.verify_block(args.block)
        if is_consistent:
            print(f"✅ Block {args.block} is consistent")
        else:
            print(f"❌ Block {args.block} has discrepancies:")
            for entity, details in discrepancies.items():
                print(f"\n{entity}:")
                print(f"  ClickHouse count: {details['clickhouse']}")
                print(f"  Nebula count: {details['nebula']}")
                print(f"  Difference: {details['difference']}")
    
    elif args.start_block is not None and args.end_block is not None:
        for block_number in range(args.start_block, args.end_block + 1):
            print(f"\n=== Verifying block {block_number} ===")
            is_consistent, discrepancies = verifier.verify_block(block_number)
            if is_consistent:
                print(f"✅ Block {block_number} is consistent")
            else:
                print(f"❌ Block {block_number} has discrepancies:")
                for entity, details in discrepancies.items():
                    print(f"\n{entity}:")
                    print(f"  ClickHouse count: {details['clickhouse']}")
                    print(f"  Nebula count: {details['nebula']}")
                    print(f"  Difference: {details['difference']}")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()