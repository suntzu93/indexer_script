!#/bin/bash

#install python
apt update
apt-get -y install python3-pip
apt-get -y install jq
apt-get -y install tmux

#install libraries
pip3 install request
pip3 install subprocess.run