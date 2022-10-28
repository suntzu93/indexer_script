**Script to execute graph command. Can run on any where, just need to connect indexer-manager !** 
```
1. git clone https://github.com/suntzu93/indexer_script.git
2. cd indexer_script
3. chmod +X install.sh
4. ./install.sh
5. Change data in config.py file
6. python3 main.py

# Run below background

tmux new -s indexer_script
python3 main.py

# Run below background for manager_actions

tmux new -s manage_action
flask --app manage_actions.py run --host 0.0.0.0 --port 5502

```
**config.py**

`token : Connect wallet and generate token on` <a href="https://graphindexer.co" target="_blank">graphindexer.io</a> 

`network : Choose the right network (mainnet / testnet)` 

`indexer_management_url : Indexer management API (default port 18000)`

`indexer_graph : graph-indexer cli` [Detail](https://github.com/graphprotocol/indexer)


***If your server did not install graph-indexer then follow commands***
```


npm install -g @graphprotocol/indexer-cli --unsafe-perm=true
yarn global add @graphprotocol/graph-cli --prefix /usr/local

```
