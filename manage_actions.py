from flask import Flask, request
import subprocess
import config
from flask_cors import CORS
import requests

import const

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
        else:
            return const.TOKEN_ERROR
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
        else:
            return const.TOKEN_ERROR
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
        if token != config.token:
            return const.TOKEN_ERROR

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


@app.route('/streamLog', methods=['POST'])
def stream_log():
    token = request.form.get("token")

    def tail(f, lines=500):
        total_lines_wanted = lines

        BLOCK_SIZE = 1024
        f.seek(0, 2)
        block_end_byte = f.tell()
        lines_to_go = total_lines_wanted
        block_number = -1
        blocks = []
        while lines_to_go > 0 and block_end_byte > 0:
            if block_end_byte - BLOCK_SIZE > 0:
                f.seek(block_number * BLOCK_SIZE, 2)
                blocks.append(f.read(BLOCK_SIZE))
            else:
                f.seek(0, 0)
                blocks.append(f.read(block_end_byte))
            lines_found = blocks[-1].count(b'\n')
            lines_to_go -= lines_found
            block_end_byte -= BLOCK_SIZE
            block_number -= 1
        all_read_text = b''.join(reversed(blocks))
        return b'\n'.join(all_read_text.splitlines()[-total_lines_wanted:])

    if token == config.token:
        f = open(config.agent_log, 'rb')
        return tail(f)
    else:
        return const.TOKEN_ERROR
    return ""


@app.route('/getHealthy', methods=['POST'])
def get_healthy_subgraph():
    try:
        token = request.form.get("token")
        subgraphs = request.form.get("subgraphs")
        if token == config.token:
            graphql_healthy_subgraph = """
                            { indexingStatuses(subgraphs: [%s]) { subgraph synced health fatalError {message deterministic block { number }} }}}
                            """ % subgraphs
            response = requests.post(url=config.indexer_node_rpc,
                                     json={"query": graphql_healthy_subgraph})
            json_data = response.json()
            return json_data
        else:
            return const.TOKEN_ERROR
    except Exception as e:
        print(e)
        return "ERROR"


@app.route('/restartAgent', methods=['POST'])
def restart_agent():
    try:
        token = request.form.get("token")
        if token == config.token:
            cmd_restart_agent = f"{config.agent_restart_cmd}"

            print("Execute cmd : " + cmd_restart_agent)
            subprocess.run([cmd_restart_agent], shell=True, check=True)
            return "OK"
        else:
            return const.TOKEN_ERROR
    except Exception as e:
        print(e)
        return "ERROR"
