!#/bin/bash

#install python
apt update
apt-get -y install python3-pip
apt-get -y install jq
apt-get -y install tmux
chmod +x run_indexer_script.sh

#install libraries
pip3 install request
pip3 install subprocess.run
pip3 install flask
pip3 install -U flask-cors