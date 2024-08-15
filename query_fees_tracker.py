import sqlite3
import time
import threading
import logging
from typing import List, Dict
import psycopg2
import requests
import config
from flask import request, jsonify

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_fees_table():
    conn = sqlite3.connect('fees_database.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS QueryFees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ipfsHash TEXT,
        allocationId TEXT,
        fees REAL,
        timestamp INTEGER
    )
    ''')
    conn.commit()
    conn.close()

def get_allocations_with_fees() -> List[Dict]:
    logging.info("Getting allocations with fees...")
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

def get_ipfs_hashes(allocations: List[Dict]) -> Dict[str, str]:
    logging.info("Getting IPFS hashes...")
    ipfs_hash_map = {}
    skip = 0
    while True:
        query = f'''
        {{
          indexer(id:"{config.indexer_address.lower()}") {{
            allocations(first:1000, skip:{skip}) {{
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
            allocations_data = data["data"]["indexer"]["allocations"]
            
            if not allocations_data:
                break
            
            ipfs_hash_map.update({alloc["id"].lower(): alloc["subgraphDeployment"]["ipfsHash"] 
                                  for alloc in allocations_data})
            
            skip += len(allocations_data)
            logging.info(f"Fetched {skip} allocations so far")
            
            if len(allocations_data) < 1000:
                break
        except Exception as e:
            logging.error(f"Error getting IPFS hashes: {e}")
            break
    
    logging.info(f"Total allocations fetched: {len(ipfs_hash_map)}")
    return ipfs_hash_map

def save_fees_data():
    create_fees_table()
    while True:
        try:
            logging.info("Starting save_fees_data cycle")
            allocations = get_allocations_with_fees()
            logging.info(f"Retrieved {len(allocations)} allocations with fees")
            
            ipfs_hash_map = get_ipfs_hashes(allocations)
            logging.info(f"Retrieved IPFS hashes for {len(ipfs_hash_map)} allocations")
            
            current_time = int(time.time())
            ten_minutes_ago = current_time - 600

            conn = sqlite3.connect('fees_database.db')
            cursor = conn.cursor()

            # Check if data has been inserted in the last 10 minutes
            cursor.execute('''
            SELECT COUNT(*) FROM QueryFees WHERE timestamp >= ?
            ''', (ten_minutes_ago,))
            recent_inserts = cursor.fetchone()[0]

            if recent_inserts > 0:
                logging.info("Data has been inserted in the last 10 minutes, skipping this cycle")
            else:
                for alloc in allocations:
                    ipfs_hash = ipfs_hash_map.get(alloc["id"].lower(), "Unknown")
                    
                    # Insert new fees directly
                    logging.info(f"Inserting new allocation {alloc['id']} with fees: {alloc['fees']}")
                    cursor.execute('''
                    INSERT INTO QueryFees (ipfsHash, allocationId, fees, timestamp)
                    VALUES (?, ?, ?, ?)
                    ''', (ipfs_hash, alloc["id"], alloc["fees"], current_time))

                conn.commit()
                logging.info(f"Saved fees data for {len(allocations)} allocations")

            conn.close()

            # Sleep for 10 minutes
            logging.info("Sleeping for 10 minutes before next cycle")
            time.sleep(600)
        except Exception as e:
            logging.error(f"Error in save_fees_data: {str(e)}")
            # Sleep for 1 minute before retrying
            logging.info("Sleeping for 1 minute before retrying due to error")
            time.sleep(60)

def get_query_fees():
    try:
        # Check token
        token = request.form.get('token')
        if token != config.token:
            return jsonify({"error": "Invalid token"}), 401

        ipfs_hash = request.form.get('ipfsHash')
        allocation_id = request.form.get('allocationId')
        start_time = request.form.get('startTime')
        end_time = request.form.get('endTime')

        conn = sqlite3.connect('fees_database.db')
        cursor = conn.cursor()

        query = "SELECT * FROM QueryFees WHERE 1=1"
        params = []

        if ipfs_hash:
            query += " AND ipfsHash = ?"
            params.append(ipfs_hash)
        if allocation_id:
            query += " AND allocationId = ?"
            params.append(allocation_id)
        if start_time:
            query += " AND timestamp >= ?"
            params.append(int(start_time))
        if end_time:
            query += " AND timestamp <= ?"
            params.append(int(end_time))

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        result = [
            {
                "id": row[0],
                "ipfsHash": row[1],
                "allocationId": row[2],
                "fees": row[3],
                "timestamp": row[4]
            }
            for row in rows
        ]

        return jsonify(result)
    except Exception as e:
        logging.error(f"Error in get_query_fees: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Add this function to your Flask app in manage_actions.py
def add_get_query_fees_route(app):
    app.add_url_rule('/getQueryFees', 'get_query_fees', get_query_fees, methods=['POST'])