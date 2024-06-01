import sqlite3
import requests
import yaml
import time


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
    conn.commit()
    conn.close()


def fetch_subgraphs():
    url = 'https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-arbitrum'
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


def start_manage_graft():
    init_db()
    while True:
        # only call for first time
        if is_db_has_data():
            subgraphs = fetch_100_subgraphs()
        else:
            subgraphs = fetch_subgraphs()
        print("Total subgraphs : " + str(len(subgraphs)))
        graft_data = process_subgraphs(subgraphs)
        save_graft_data(graft_data)

        # sleep 6hrs
        time.sleep(6 * 60 * 60)
