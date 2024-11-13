import sqlite3
import requests
import yaml
import time
from datetime import datetime, timedelta
import config

def init_db():
    conn = sqlite3.connect('subgraph_database.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS manage_graft (
        version TEXT,
        ipfs TEXT,
        graft_ipfs TEXT,
        graft_block TEXT
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS last_fetch (
        id INTEGER PRIMARY KEY,
        timestamp DATETIME
    )
    ''')
    # Insert initial timestamp if table is empty
    cursor.execute('INSERT OR IGNORE INTO last_fetch (id, timestamp) VALUES (1, ?)', (datetime.min.isoformat(),))
    conn.commit()
    conn.close()


def fetch_subgraphs():
    url = config.indexer_agent_network_subgraph_endpoint
    all_subgraphs = []
    skip = 0
    while True:
        query_subgraph = f'''
                {{
                  subgraphs(first:1000, skip:{skip}, where:{{currentSignalledTokens_gt:990000000000000000}}) {{
                    currentSignalledTokens
                    currentVersion {{
                      subgraphDeployment {{
                        ipfsHash
                        versions {{
                          id
                        }}
                      }}
                    }}
                  }}
                }}
                '''
        response = requests.post(url, json={'query': query_subgraph})
        data = response.json()
        subgraphs = data['data']['subgraphs']
        all_subgraphs.extend(subgraphs)
        if len(subgraphs) < 1000:
            break
        skip += 1000

    return all_subgraphs


def fetch_100_subgraphs():
    url = 'https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-arbitrum'
    all_subgraphs = []
    skip = 0
    while True:
        query_subgraph = f'''
                {{
                    subgraphs(first:100,where:{{currentSignalledTokens_gt:990000000000000000}},orderBy:updatedAt,orderDirection:desc){{
                    currentSignalledTokens
                    currentVersion {{
                      subgraphDeployment {{
                        ipfsHash
                        versions {{
                          id
                        }}
                      }}
                    }}
                  }}
                }}
                '''
        response = requests.post(url, json={'query': query_subgraph})
        data = response.json()
        subgraphs = data['data']['subgraphs']
        all_subgraphs.extend(subgraphs)
        if len(subgraphs) < 1000:
            break
        skip += 1000

    return all_subgraphs


def fetch_with_timeout(url, timeout):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None


def process_subgraphs(subgraphs):
    graft_data = []
    i = 0
    for subgraph in subgraphs:
        print("number : " + str(i))
        i += 1
        ipfsHash = subgraph['currentVersion']['subgraphDeployment']['ipfsHash']
        version = subgraph['currentVersion']['subgraphDeployment']['versions'][0]["id"]
        fetch_graft_data(version, ipfsHash, graft_data)
    return graft_data


def fetch_graft_data(version, ipfsHash, graft_data):
    stillLoadGraft = True
    print("Loading graft for subgraph : " + str(ipfsHash) + " - version : " + version)
    while stillLoadGraft:
        try:
            ipfsUrl = f"https://ipfs.network.thegraph.com/api/v0/cat?arg={ipfsHash}"
            textResponse = fetch_with_timeout(ipfsUrl, timeout=15)
            if textResponse:
                responseObj = yaml.safe_load(textResponse)
                if "graft" in responseObj:
                    base = responseObj['graft']['base']
                    block = responseObj['graft']['block']
                    graft_data.append({
                        'version': version,
                        'ipfs': ipfsHash,
                        'graft_ipfs': base,
                        'graft_block': block
                    })
                    ipfsHash = base
                else:
                    print("NO GRAFT !")
                    stillLoadGraft = False
            else:
                stillLoadGraft = False
        except Exception as e:
            print(e)
            break


def save_graft_data(graft_data):
    conn = sqlite3.connect('subgraph_database.db')
    cursor = conn.cursor()

    for data in graft_data:
        try:
            cursor.execute('''
                    SELECT COUNT(*) FROM manage_graft WHERE graft_ipfs = ?
                    ''', (data['graft_ipfs'],))
            count = cursor.fetchone()[0]

            if count == 0:
                cursor.execute('''
                        INSERT INTO manage_graft (version, ipfs, graft_ipfs, graft_block) 
                        VALUES (?, ?, ?, ?)
                        ''', (data['version'], data['ipfs'], data['graft_ipfs'], data['graft_block']))
        except Exception as e:
            print("save_graft_data : " + str(e))

    conn.commit()
    conn.close()


def is_db_has_data():
    conn = sqlite3.connect('subgraph_database.db')
    cursor = conn.cursor()

    try:
        cursor.execute('''SELECT COUNT(*) FROM manage_graft ''')
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        print("save_graft_data : " + str(e))
    cursor.close()
    conn.close()


def get_last_fetch_time():
    conn = sqlite3.connect('subgraph_database.db')
    cursor = conn.cursor()
    cursor.execute('SELECT timestamp FROM last_fetch ORDER BY timestamp DESC LIMIT 1')
    result = cursor.fetchone()
    conn.close()
    return datetime.fromisoformat(result[0]) if result else None


def update_last_fetch_time():
    conn = sqlite3.connect('subgraph_database.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE last_fetch SET timestamp = ? WHERE id = 1', (datetime.now().isoformat(),))
    conn.commit()
    conn.close()


def start_manage_graft():
    init_db()
    while True:
        if not is_db_has_data():
            subgraphs = fetch_subgraphs()
            print("Database empty. Fetching all subgraphs.")
        else:
            last_fetch_time = get_last_fetch_time()
            current_time = datetime.now()

            if last_fetch_time is None or (current_time - last_fetch_time) > timedelta(hours=1):
                subgraphs = fetch_100_subgraphs()
                print("Fetching 100 most recent subgraphs.")
                update_last_fetch_time()
            else:
                print("Less than 1 hour since last fetch. Skipping...")
                time.sleep(60 * 60)  # Sleep for 1 hour before checking again
                continue

        print("Total subgraphs : " + str(len(subgraphs)))
        graft_data = process_subgraphs(subgraphs)
        save_graft_data(graft_data)

        # Sleep for 6 hours before checking again
        time.sleep(6 * 60 * 60)
