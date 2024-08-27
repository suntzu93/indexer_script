import sqlite3
import time
import requests
import psycopg2
import config
import logging
from typing import List, Dict
import const
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up file handler
file_handler = logging.FileHandler('subgraph_syncing.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Get the root logger and add the file handler
root_logger = logging.getLogger()
root_logger.addHandler(file_handler)

def create_subgraph_status_table():
    conn = sqlite3.connect('subgraph_database.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS SubgraphStatus (
        ipfsHash TEXT PRIMARY KEY,
        latestBlock INTEGER,
        health TEXT
    )
    ''')
    conn.commit()
    conn.close()

def get_allocations_with_fees() -> List[Dict]:
    print("Getting allocations with fees...")
    try:
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            database=config.agent_database,
            user=config.username,
            password=config.password
        )
        cursor = conn.cursor()
        cursor.execute('''
        SELECT lower(ar.allocation) as "Allocation ID", sum(fees) / 10^18 as "Fees"
        FROM public.allocation_summaries als
        JOIN public.allocation_receipts ar on als.allocation = ar.allocation 
        WHERE als."closedAt" is null
        GROUP BY lower(ar.allocation)
        ''')
        allocations = [{"id": row[0], "fees": float(row[1])} for row in cursor.fetchall()]
        conn.close()
        return [alloc for alloc in allocations if alloc["fees"] > 10]
    except Exception as e:
        logging.error(f"Error getting allocations: {e}")
        return []

def get_ipfs_hashes(allocations: List[Dict]) -> List[str]:
    print("Getting IPFS hashes...")
    query = f'''
    {{
      indexer(id:"{config.indexer_address.lower()}") {{
        allocations(first:1000) {{
          id
          subgraphDeployment {{
            ipfsHash
          }}
        }}
      }}
    }}
    '''
    try:
        response = requests.post(config.indexer_agent_network_subgraph_endpoint, json={"query": query})
        response.raise_for_status()
        data = response.json()
        allocation_map = {alloc["id"].lower(): alloc["subgraphDeployment"]["ipfsHash"] 
                          for alloc in data["data"]["indexer"]["allocations"]}
        return [allocation_map[alloc["id"].lower()] for alloc in allocations if alloc["id"].lower() in allocation_map]
    except Exception as e:
        logging.error(f"Error getting IPFS hashes: {e}")
        return []

def get_indexing_statuses(ipfs_hashes: List[str]) -> List[Dict]:
    print("Getting indexing statuses...")
    query = f'''
    {{
      indexingStatuses(subgraphs: {json.dumps([hash.strip() for hash in ipfs_hashes])}) {{
        subgraph
        health
        chains {{
          network
          latestBlock {{ number }}
          chainHeadBlock {{ number }}
        }}
      }}
    }}
    '''
    try:
        response = requests.post(config.indexer_node_rpc, json={"query": query})
        response.raise_for_status()
        return response.json()["data"]["indexingStatuses"]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting indexing statuses: {e}")
        if e.response is not None:
            logging.error(f"Response content: {e.response.content}")
        return []

def update_subgraph_status(statuses: List[Dict]):
    logging.info("Updating subgraph status...")
    conn = sqlite3.connect('subgraph_database.db')
    cursor = conn.cursor()
    
    for status in statuses:
        ipfs_hash = status["subgraph"]
        health = status["health"]
        latest_block = int(status["chains"][0]["latestBlock"]["number"])
        chain_head_block = int(status["chains"][0]["chainHeadBlock"]["number"])
        network = status["chains"][0]["network"]
        
        logging.info(f"--------------------------------")
        logging.info(f"Processing subgraph: {ipfs_hash}")
        logging.info(f"Network: {network}, Health: {health}")
        logging.info(f"Latest Block: {latest_block}, Chain Head Block: {chain_head_block}")
        
        cursor.execute("SELECT latestBlock FROM SubgraphStatus WHERE ipfsHash = ?", (ipfs_hash,))
        result = cursor.fetchone()
        
        if result is None:
            logging.info(f"New subgraph detected. Inserting {ipfs_hash} into database.")
            cursor.execute("INSERT INTO SubgraphStatus (ipfsHash, latestBlock, health) VALUES (?, ?, ?)",
                           (ipfs_hash, latest_block, health))
        else:
            old_latest_block = int(result[0])
            logging.info(f"Existing subgraph. Old latest block: {old_latest_block}")
            threshold = get_threshold(network)
            blocks_behind = chain_head_block - latest_block
            logging.info(f"Blocks behind: {blocks_behind}, Threshold: {threshold}")
            
            if blocks_behind > threshold:
                alert_message = f"Syncing Warning : {ipfs_hash} - {network} - {blocks_behind} blocks behind"
                logging.warning(alert_message)
                send_alert_msg(alert_message)
            else:
                logging.info("Blocks behind within threshold. No alert sent.")
            
            logging.info(f"Updating subgraph {ipfs_hash} in database.")
            cursor.execute("UPDATE SubgraphStatus SET latestBlock = ?, health = ? WHERE ipfsHash = ?",
                           (latest_block, health, ipfs_hash))
    
    conn.commit()
    conn.close()
    logging.info("Subgraph status update completed.")

def get_threshold(network: str) -> int:
    if network in ["mainnet","gnosis"]:
        return 100
    elif network in ["arbitrum-one", "optimism"]:
        return 3500
    elif network == "matic":
        return 300
    else:
        return 500

def send_alert_msg(message: str):
    try:
        params = {
            'chatId': config.chat_id,
            'msg': message,
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(const.API_ALERT_RPC, data=params, headers=headers)
        if response.status_code == 200 and response.json()["code"] == 0:
            logging.info(f"Alert sent successfully: {message}")
        else:
            logging.error(f"Failed to send alert: {message}")
    except Exception as e:
        logging.error(f"Error sending alert: {e}")

def monitor_subgraph_syncing():
    print("Starting monitor subgraph syncing...")
    create_subgraph_status_table()

    print("Monitor subgraph syncing started")
    
    while True:
        try:
            allocations = get_allocations_with_fees()
            logging.info(f"Got {len(allocations)} allocations with fees")
            
            ipfs_hashes = get_ipfs_hashes(allocations)
            logging.info(f"Got {len(ipfs_hashes)} IPFS hashes")
            
            indexing_statuses = get_indexing_statuses(ipfs_hashes)
            logging.info(f"Got {len(indexing_statuses)} indexing statuses")
            
            update_subgraph_status(indexing_statuses)
            
            logging.info("Subgraph syncing check completed")
            
            # Sleep for 5 minutes
            time.sleep(300)
        except Exception as e:
            logging.error(f"Error in monitor_subgraph_syncing: {str(e)}")
            logging.exception("Exception details:")
            # Sleep for 1 minute before retrying
            time.sleep(60)

if __name__ == "__main__":
    monitor_subgraph_syncing()