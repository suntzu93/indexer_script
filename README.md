**Script to execute graph command. Can run on any where, just need to connect indexer-manager !** 
```
1. git clone https://github.com/suntzu93/indexer_script.git
2. cd indexer_script
3. ./install.sh
4. Change data in .config file
5. python3 main.py

# Run below background

tmux new -s indexer_script
python3 main.py
```

***If your server did not install graph-indexer then follow commands***
```


npm install -g @graphprotocol/indexer-cli --unsafe-perm=true
yarn global add @graphprotocol/graph-cli --prefix /usr/local

```
