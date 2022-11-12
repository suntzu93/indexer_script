!#/bin/bash


#install python
apt update
apt-get -y install python3-pip
apt-get -y install jq
apt-get -y install tmux
chmod +x run_indexer_script.sh
apt-get install wget

#install libraries
pip3 install request
pip3 install subprocess.run
pip3 install flask
pip3 install -U flask-cors

#download config
wget https://gist.githubusercontent.com/suntzu93/8692faff0a60fc8acc1c3e09daedd3a3/raw/19f4cf959b2a19edd499b8be00c170076bf4aa98/config.py
wget https://gist.githubusercontent.com/suntzu93/8b3f1969e84125fdf6f02ea2902cb58e/raw/01016fc199a321425b7719c68aebe23b0caaf2ce/graphman_config.toml