# indexer_script

**Script to execute graph command. Can run on any where, just need to connect indexer-manager !** 

## 1. Installation
```
cd ~
git clone https://github.com/suntzu93/indexer_script.git
cd indexer_script
wget -O - https://gist.githubusercontent.com/suntzu93/901543b5a37aba66de2373d87d968669/raw/install.sh | bash
```
## 2. Change data in `config.py` & `graphman_config.toml`
```
cd ~/indexer_script
nano config.py
nano graphman_config.toml
#edit & save your config.py and graphman_config.toml
```
## 3. Run a script
```
cd ~/indexer_script
./run_indexer_script.sh
#script running below background
```
>Default Port is 5502, for your endpoint it must be `https` before input to <a href="https://graphindexer.co" target="_blank">graphindexer.io</a>

## 4. For update latest code
```
cd ~/indexer_script
git pull
./run_indexer_script.sh
```

## 5. How to change data in `config.py` file with example 
>**config.py**

|                 Attempt                 |                                                   #Description                                                   |
|:---------------------------------------:|:----------------------------------------------------------------------------------------------------------------:|
|                  token                  |    Connect wallet and generate token on <a href="https://graphindexer.co" target="_blank">graphindexer.io</a>    |
|                 network                 |                                   Choose the right network (mainnet / testnet)                                   |
|         indexer_management_url          |                                   Indexer management API (default port 18000)                                    |
|            indexer_node_rpc             |                                   Graph Index Node Server (default port 8030)                                    |
| indexer_agent_network_subgraph_endpoint | **testnet** : https://gateway.testnet.thegraph.com/network or **mainnet** : https://gateway.thegraph.com/network |
|                node_rpc                 |                            **testnet** : Goerli RPC or **mainnet** : eth mainnet rpc                             |
|              indexer_graph              |                          indexer cli [Detail](https://github.com/graphprotocol/indexer)                          |
|             indexer_address             |                                                 Indexer address                                                  |
|              graphman_cli               |                                                   Graphman cli                                                   |
|          graphman_config_file           |      Graphman config file [Detail](https://github.com/graphprotocol/graph-node/blob/master/docs/config.md)       |
|                rpc_list                 |       To monitor rpc healthy , only support EVM rpc, cosmos and near, format ["http://rpc1","http://rpc2"]       |
|                  host                   |                                     Should be 0.0.0.0 to access from network                                     |
|                  port                   |                                        Indexer script port (default 5502)                                        |

*example:*

```
# *** Required ***
token = "0x14958542867fc280a3879f23843f389a78d..."
network = "testnet"
indexer_management_url = "http://127.0.0.1:18000"
indexer_graph = "graph"
host = "0.0.0.0"
port = 5502


# Optional

# For get POI and health of subgraphs
indexer_node_rpc = "http://127.0.0.1:8030/graphql"
indexer_agent_network_subgraph_endpoint = "https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-goerli"
node_rpc = "https://eth-goerli.g.alchemy.com/v2/xxxxx"
indexer_address = "0x...."


# For view agent log and restart agent.
agent_log = "/root/.pm2/logs/indexer-agent-out.log"
agent_restart_cmd = "pm2 restart indexer_agent"

# For graphman
graphman_cli = "/root/graph-node/target/debug/graphman"
graphman_config_file = "graphman_config.toml"

# For monitor RPCs
# To get chat_id , enter /start in this bot https://t.me/MonitorRpcBot
chat_id = 1234567
# notify when your node is 100 blocks slower than chain head
threshold_block_behind = 100

#for monitor query reward
db_host = "127.0.0.1"
db_port = 5432
agent_database = "graph_indexer_agent"
username = "admin"
password = "admin"

#Support chain tracking near, cosmos and erc20 chains.
# List of erc20 chain_ids that support monitor : 1,5,10,56,99,100,122,136,250,42161,42170,42220,43114,1313161554,1666600000
rpc_list = {
    "erc20": [
        "http://127.0.0.1:8555",
        "http://127.0.0.1:8554",
    ],
    "near": [
        "https://rpc.mainnet.near.org/status"
    ],
    "cosmos": [
        "https://cosmos-rpc.polkachu.com/status"
    ]
}
```

***If your server did not install graph-indexer then follow commands***
```
npm install -g @graphprotocol/indexer-cli --unsafe-perm=true
yarn global add @graphprotocol/graph-cli --prefix /usr/local

```
