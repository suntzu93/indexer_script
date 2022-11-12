from flask import Flask, request
import subprocess
import config
from flask_cors import CORS
import requests
import logging
import const
import json
import os.path

app = Flask(__name__)
CORS(app)

logging.basicConfig(filename='indexer_script.log', level=logging.INFO)


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
        logging.error("get_actions: " + str(e))
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
        logging.error("cancel_actions: " + str(e))
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
        logging.error("get_poi: " + str(e))
        return action_output


@app.route('/streamLog', methods=['POST'])
def stream_log():
    try:
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
    except Exception as e:
        print(e)
        logging.error("get_poi: " + str(e))
        return ""


@app.route('/getIndexingStatus', methods=['POST'])
def get_healthy_subgraph():
    try:
        token = request.form.get("token")
        # subgraphs = request.form.get("subgraphs")
        if token == config.token:
            # graphql_healthy_subgraph = """
            #                 { indexingStatuses(subgraphs: [%s]) { subgraph synced health node fatalError {message deterministic block { number }} chains {latestBlock {number} chainHeadBlock {number}}}}""" % subgraphs
            graphql_healthy_subgraph = "{ indexingStatuses { subgraph synced health node fatalError {message deterministic block { number }} chains {network latestBlock {number} chainHeadBlock {number}}}}"
            response = requests.post(url=config.indexer_node_rpc,
                                     json={"query": graphql_healthy_subgraph})
            json_data = response.json()
            return json_data
        else:
            return const.TOKEN_ERROR
    except Exception as e:
        print(e)
        logging.error("get_healthy_subgraph: " + str(e))
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
        logging.error("restart_agent: " + str(e))
        return "ERROR"


@app.route('/graphman', methods=['POST'])
def graphman():
    try:
        token = request.form.get("token")
        command = request.form.get("command")
        ipfsHash = request.form.get("ipfsHash")
        graphNode = request.form.get("graphNode")
        rewindBlock = request.form.get("rewindBlock")
        rewindBlockHash = request.form.get("rewindBlockHash")
        if token == config.token:
            logging.info(command + " " + ipfsHash + " " + str(graphNode) + " " + str(rewindBlock) + " " + str(rewindBlockHash))
            graphman_cmd = ""
            if command == const.GRAPHMAN_REASSIGN:
                graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} {command} {ipfsHash} {graphNode}"
            elif command == const.GRAPHMAN_UNASSIGN:
                graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} {command} {ipfsHash}"
            elif command == const.GRAPHMAN_REMOVE:
                graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} drop --force  {ipfsHash}"
            elif command == const.GRAPHMAN_REWIND:
                graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} {command} {rewindBlockHash} {rewindBlock} {ipfsHash}"

            if len(graphman_cmd) > 0:
                logging.info("graphman_cmd: " + graphman_cmd)
                result = subprocess.run([graphman_cmd], shell=True, check=True,
                                         stdout=subprocess.PIPE,
                                         universal_newlines=True)
                output = result.stdout
                logging.info("output: " + str(output))
            return "OK"
        else:
            return const.TOKEN_ERROR
    except Exception as e:
        print(e)
        logging.error("graphman: " + str(e))
        return "ERROR"

@app.route('/verify', methods=['POST'])
def verify():
    try:
        verifyStatus = {
            "token": False,
            "indexer_management_url": False,
            "indexer_node_rpc": False,
            "indexer_agent_network_subgraph_endpoint": False,
            "node_rpc": False,
            "indexer_graph": False,
            "indexer_address": False,
            "agent_log": False

        }
        # verify token
        token = request.form.get("token")
        verifyStatus["token"] = token == config.token

        # verify indexer_management_url
        request_indexer_management_url = requests.get(url=config.indexer_management_url)
        if request_indexer_management_url.status_code == 200:
            response = request_indexer_management_url.text
            verifyStatus["indexer_management_url"] = response == "Ready to roll!"

        # verify indexer_node_rpc
        try:
            indexing_status = '{indexingStatuses{chains{latestBlock{number}}}}'
            graphql_indexing_status = {
                "query": indexing_status
            }
            indexing_status_response = requests.post(url=config.indexer_node_rpc,
                                                     json=graphql_indexing_status)
            if indexing_status_response.status_code == 200:
                indexingStatuses = indexing_status_response.json()
                number = indexingStatuses["data"]["indexingStatuses"][0]["chains"][0]["latestBlock"]["number"]
                verifyStatus["indexer_node_rpc"] = int(number) > 0
        except Exception as e:
            print(e)
            logging.error("verify indexer_node_rpc : " + str(e))

        # verify indexer_agent_network_subgraph_endpoint
        try:
            network_mainnet = '{network(id:"mainnet"){id}}'
            graphql_network_mainnet = {
                "query": network_mainnet
            }
            indexing_status_response = requests.post(url=config.indexer_agent_network_subgraph_endpoint,
                                                     json=graphql_network_mainnet)
            if indexing_status_response.status_code == 200:
                network_response = indexing_status_response.json()
                networkId = network_response["data"]["network"]["id"]
                verifyStatus["indexer_agent_network_subgraph_endpoint"] = networkId == "mainnet"
        except Exception as e:
            print(e)
            logging.error("verify indexer_agent_network_subgraph_endpoint : " + str(e))

        # verify node_rpc
        try:
            playload_blockNumber = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": []
            }
            headers = {
                "accept": "application/json",
                "content-type": "application/json"
            }
            responseBlockHash = requests.post(url=config.node_rpc,
                                              json=playload_blockNumber,
                                              headers=headers)
            block_hash_data = responseBlockHash.json()
            verifyStatus["node_rpc"] = int(block_hash_data["result"], 16) > 0
        except Exception as e:
            print(e)
            logging.error("verify node_rpc : " + str(e))

        # verify indexer_graph
        try:
            get_version_graph = f"{config.indexer_graph} --version"

            print("Execute cmd : " + get_version_graph)
            process = subprocess.run([get_version_graph], shell=True, check=True,
                                     stdout=subprocess.PIPE,
                                     universal_newlines=True)
            action_output = process.stdout
            if action_output is not None and len(action_output) > 0 and "0." in action_output:
                verifyStatus["indexer_graph"] = True

        except Exception as e:
            print(e)
            logging.error("verify node_rpc : " + str(e))

        # verify indexer_address
        verifyStatus["indexer_address"] = len(config.indexer_address) == 42 and config.indexer_address.startswith(
            "0x")

        # verify agent_log
        verifyStatus["agent_log"] = os.path.exists(config.agent_log)

        jsonString = json.dumps(verifyStatus)
        logging.info("verify : " + jsonString)
        return jsonString
    except Exception as e:
        print(e)
        logging.error("verify: " + str(e))
        return "ERROR"
