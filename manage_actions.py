from flask import Flask
from flask import request
import subprocess
import config
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)


@app.route('/actions', methods=['POST'])
def get_actions():
    try:
        # queued | approved | pending | success | failed | canceled
        actionStatus = request.form.get("actionStatus")
        token = request.form.get("token")
        # Only support from indexer-agent version > v0.20.5
        limit = request.form.get("limit")
        action_output = []
        if token == config.token:
            cmd_get_actions = f"{config.indexer_graph} indexer actions get --status {actionStatus} --output=json --limit={limit} "

            print("Execute cmd : " + cmd_get_actions)
            process = subprocess.run([cmd_get_actions], shell=True, check=True,
                                     stdout=subprocess.PIPE,
                                     universal_newlines=True)
            action_output = process.stdout
        return action_output
    except Exception as e:
        print(e)
        return []


@app.route('/cancelAction', methods=['POST'])
def cancel_actions():
    try:
        actionId = request.form.get("actionId")
        token = request.form.get("token")
        action_output = []
        if token == config.token:
            cmd_cancel_actions = f"{config.indexer_graph} indexer actions cancel {actionId} --output=json"

            print("Execute cmd : " + cmd_cancel_actions)
            process = subprocess.run([cmd_cancel_actions], shell=True, check=True,
                                     stdout=subprocess.PIPE,
                                     universal_newlines=True)
            action_output = process.stdout

        return action_output
    except Exception as e:
        print(e)
        return []


@app.route('/getPoi', methods=['POST'])
def get_poi():
    try:
        token = request.form.get("token")
        deployment = request.form.get("deployment")
        blockBroken = request.form.get("blockBroken")
        action_output = "NO VALID POI"
        if token == config.token:
            graphql_startBlock = """
                {
                  epoches(where: {startBlock_lt: %s, endBlock_gt: %s}) {
                    startBlock
                  }
                }
                """
            response = requests.post(url=config.indexer_agent_network_subgraph_endpoint,
                                     json={"query": graphql_startBlock % (blockBroken, blockBroken)})
            json_data = response.json()
            if response.status_code == 200:
                startBlock = json_data["data"]["epoches"][0]["startBlock"]
                startBlockHex = hex(startBlock)

                playload_blockHash = {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [str(startBlockHex), False]
                }
                headers = {
                    "accept": "application/json",
                    "content-type": "application/json"
                }
                responseBlockHash = requests.post(url=config.node_rpc,
                                                  json=playload_blockHash,
                                                  headers=headers)
                block_hash_data = responseBlockHash.json()
                block_hash = block_hash_data["result"]["hash"]

                proof_of_indexing = 'query{proofOfIndexing(subgraph:"%s",blockHash:"%s",blockNumber:%s,indexer:"%s")}' % (
                    deployment, block_hash, startBlock, config.indexer_address)

                graphql_proof_of_indexing = {
                    "query": proof_of_indexing
                }

                poi_response = requests.post(url=config.indexer_node_rpc,
                                             json=graphql_proof_of_indexing)
                if poi_response.status_code == 200:
                    json_poi = poi_response.json()
                    return json_poi["data"]["proofOfIndexing"]
        return action_output
    except Exception as e:
        print(e)
        return action_output
