#!/bin/bash
{ # try
    tmux kill-session -t indexer_script
} || { # catch
    echo "No session to kill !"
}

tmux new-session -d -s indexer_script 'python3 main.py'
echo "Run indexer_script with default port is 5502 !"