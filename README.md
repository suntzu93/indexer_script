**Script to execute graph command. Can run on any where, just need to connect indexer-manager !** 
```

cd ~
1. git clone https://github.com/suntzu93/indexer_script.git
2. cd indexer_script
3. chmod +X install.sh
4. ./install.sh
5. Change data in config.py file
6. python3 main.py
```
Default Port is 5502, for your endpoint it must be `https` before input to <a href="https://graphindexer.co" target="_blank">graphindexer.io</a>
```
# Run below background

cat <<'EOT' > /root/indexer_script/indexer_script.sh
#!/bin/bash
tmux new-session -d -s indexer_script 'python3 /root/indexer_script/main.py'
EOT

chmod +x /root/indexer_script/indexer_script.sh
./root/indexer_script/indexer_script.sh

#For update latest code

tmux kill-session -t indexer_script

cd ~/indexer_script
git pull

./indexer_script.sh


```
**config.py**

| Attempt | #Description    |
| :---:   | :---: |
| token | Connect wallet and generate token on <a href="https://graphindexer.co" target="_blank">graphindexer.io</a>    |
| network | Choose the right network (mainnet / testnet) |
| indexer_management_url   | Indexer management API (default port 18000) |
| indexer_node_rpc   | Graph Index Node Server (default port 8030) |
| indexer_agent_network_subgraph_endpoint   | **testnet** : https://gateway.testnet.thegraph.com/network or **mainnet** : https://gateway.thegraph.com/network |
| node_rpc   | **testnet** : Goerli RPC or **mainnet** : eth mainnet rpc |
| indexer_graph   | indexer cli [Detail](https://github.com/graphprotocol/indexer) |
| indexer_address   | Indexer address |
| host   | Should be 0.0.0.0 to access from network |
| port   | Indexer script port (default 5502) |

*example:*

```
token = "0x14958542867fc280a3879f23843f389a78d9e32539..."
network = "testnet"
indexer_management_url = "http://127.0.0.1:18000"
indexer_node_rpc = "http://127.0.0.1:8030/graphql"
indexer_agent_network_subgraph_endpoint = "https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-goerli"
node_rpc = "https://eth-goerli.g.alchemy.com/v2/<token>"
indexer_graph = "/indexer/node_modules/@graphprotocol/indexer-cli/bin/graph-indexer"
indexer_address = "0x4167eb613d784c910f5dc0f3f0515d61ec6ec8df"
agent_log = "/root/agent_log.txt"
agent_restart_cmd = "pm2 restart indexer_agent"
host = "0.0.0.0"
port = 5502
```

***If your server did not install graph-indexer then follow commands***
```


npm install -g @graphprotocol/indexer-cli --unsafe-perm=true
yarn global add @graphprotocol/graph-cli --prefix /usr/local

```
