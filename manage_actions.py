import sqlite3

from flask import Flask, request, jsonify
import subprocess
import config
from flask_cors import CORS
import requests
import logging
import const
import json
import os.path
import yaml
import pending_reward
import re

app = Flask(__name__)
CORS(app)

logging.basicConfig(filename='indexer_script.log', level=logging.INFO)


def get_chain_rpc(chain_name):
    chain_rpcs = {
        'mainnet': const.ETH_RPC_ENDPOINT,
        'goerli': const.GOERLI_RPC_ENDPOINT,
        'optimism': const.OPTIMISM_RPC_ENDPOINT,
        'bsc': const.BSC_RPC_ENDPOINT,
        'poa': const.POA_RPC_ENDPOINT,
        'gnosis': const.GNOSIS_RPC_ENDPOINT,
        'fuse': const.FUSE_RPC_ENDPOINT,
        'matic': const.POLYGON_RPC_ENDPOINT,
        'fantom': const.FANTOM_RPC_ENDPOINT,
        'arbitrum-one': const.ARBITRUM_RPC_ENDPOINT,
        'arbitrum-nova': const.ARBITRUM_NOVA_RPC_ENDPOINT,
        'celo': const.CELO_RPC_ENDPOINT,
        'avalanche': const.AVAX_RPC_ENDPOINT,
        'aurora': const.AURORA_RPC_ENDPOINT,
        'harmony': const.HARMONY_RPC_ENDPOINT,
    }
    return chain_rpcs.get(chain_name, None)


def get_block_hash(deployment, block_number):
    try:
        url_ipfs = "https://ipfs.network.thegraph.com/api/v0/cat?arg=" + deployment
        response = requests.get(url_ipfs)
        data = yaml.safe_load(response.content)
        chain_name = data["dataSources"][0]["network"]
        chain_rpc = get_chain_rpc(chain_name)
        if chain_rpc is not None:
            blockHex = hex(block_number)
            playload_blockHash = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [str(blockHex), False]
            }
            headers = {
                "accept": "application/json",
                "content-type": "application/json"
            }
            responseBlockHash = requests.post(url=chain_rpc,
                                              json=playload_blockHash,
                                              headers=headers)
            block_hash_data = responseBlockHash.json()
            if "hash" in block_hash_data["result"]:
                return block_hash_data["result"]["hash"]

        return -1
    except Exception as e:
        print("get block hash error : " + e)
        logging.error("get_block_hash: " + str(e))
        return -1


@app.route('/actions', methods=['POST'])
def get_actions():
    try:
        # queued | approved | pending | success | failed | canceled
        actionStatus = request.form.get("actionStatus")
        token = request.form.get("token")
        # Only support from indexer-agent version > v0.20.5
        limit = request.form.get("limit")
        if token == config.token:
            cmd_get_actions = f"{config.indexer_graph} indexer actions get --status {actionStatus} --output=json --limit={limit} "

            print("Execute cmd : " + cmd_get_actions)
            process = subprocess.run([cmd_get_actions], shell=True, check=True,
                                     stdout=subprocess.PIPE,
                                     universal_newlines=True)
            action_output = process.stdout
            return action_output
        else:
            return const.TOKEN_ERROR
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
            cmd_cancel_actions = f"{config.indexer_graph} indexer actions cancel {actionId} --output=json --network={config.agent_network}"
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


@app.route('/rules', methods=['POST'])
def get_rules():
    try:
        token = request.form.get("token")
        action_output = []
        if token == config.token:
            cmd_get_rules = f"{config.indexer_graph} indexer rules get all --output=json --network={config.agent_network}"
            logging.info(cmd_get_rules)
            process = subprocess.run([cmd_get_rules], shell=True, check=True,
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


@app.route('/updateRules', methods=['POST'])
def update_rules():
    try:
        token = request.form.get("token")
        deployment = request.form.get("deployment")
        autoRenewal = request.form.get("autoRenewal")
        decisionBasis = request.form.get("decisionBasis")
        allocationAmount = request.form.get("allocationAmount")
        if token == config.token:
            cmd_update_rules = f"{config.indexer_graph} indexer rules set {deployment} allocationAmount {allocationAmount} autoRenewal {autoRenewal} decisionBasis {decisionBasis} --network={config.agent_network}"
            logging.info(cmd_update_rules)
            subprocess.run([cmd_update_rules], shell=True, check=True)
            logging.info(f"Update success autoRenewal for {deployment}")
        else:
            return const.TOKEN_ERROR
        return const.SUCCESS
    except Exception as e:
        print(e)
        logging.error("get_actions: " + str(e))
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

        if config.network == "arbitrum":
            blockBrokenHex = hex(int(blockBroken))
            headers = {
                'Content-Type': 'application/json'
            }
            data_eth_getBlockByNumber = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getBlockByNumber",
                "params": [
                    blockBrokenHex,
                    False
                ]
            }
            response = requests.post(url=const.ARBITRUM_RPC_ENDPOINT,
                                     headers=headers,
                                     data=json.dumps(data_eth_getBlockByNumber))
            if response.status_code == 200:
                json_data = response.json()
                l1BlockNumberHex = json_data["result"]["l1BlockNumber"]
                l1BlockNumber = int(l1BlockNumberHex, 0)
                if l1BlockNumber > 16083151:
                    blockBroken = l1BlockNumber
                else:
                    blockBroken = 16083152
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
        print(json_data)
        if response.status_code == 200:
            if len(json_data['data']['epoches']) == 0 and 16083151 < blockBroken < 16568309:
                startBlock = 16083151
            else:
                startBlock = json_data["data"]["epoches"][0]["startBlock"]
            block_hash = get_block_hash(deployment, startBlock)

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
        return const.ERROR


@app.route('/getIndexingStatus', methods=['POST'])
def get_healthy_subgraph():
    try:
        token = request.form.get("token")
        subgraphs = request.form.get("subgraphs")
        if token == config.token:
            if subgraphs in request.form and subgraphs != "all":
                graphql_healthy_subgraph = """
                                            { indexingStatuses(subgraphs: [%s]) { subgraph synced health node fatalError {message deterministic block { number }} chains {network latestBlock {number} chainHeadBlock {number}}}}""" % subgraphs
            else:
                graphql_healthy_subgraph = "{ indexingStatuses { subgraph synced health node fatalError {message deterministic block { number }} chains {network latestBlock {number} chainHeadBlock {number}}}}"

            response = requests.post(url=config.indexer_node_rpc,
                                     json={"query": graphql_healthy_subgraph})
            indexing_status = response.json()
            reward = pending_reward.get_allocations_reward()
            indexing_status["reward"] = reward

            return json.dumps(indexing_status)
        else:
            return const.TOKEN_ERROR
    except Exception as e:
        print(e)
        logging.error("get_healthy_subgraph: " + str(e))
        return const.ERROR


@app.route('/restartAgent', methods=['POST'])
def restart_agent():
    try:
        token = request.form.get("token")
        if token == config.token:
            cmd_restart_agent = f"{config.agent_restart_cmd}"

            print("Execute cmd : " + cmd_restart_agent)
            subprocess.run([cmd_restart_agent], shell=True, check=True)
            return const.SUCCESS
        else:
            return const.TOKEN_ERROR
    except Exception as e:
        print(e)
        logging.error("restart_agent: " + str(e))
        return const.ERROR


@app.route('/graphman', methods=['POST'])
def graphman():
    try:
        token = request.form.get("token")
        command = request.form.get("command")
        ipfsHash = request.form.get("ipfsHash")
        graphNode = request.form.get("graphNode")
        rewindBlock = request.form.get("rewindBlock")
        isOffchain = int(request.form.get("isOffchain"))
        if token == config.token:
            logging.info(
                command + " " + ipfsHash + " " + str(graphNode) + " " + str(rewindBlock))
            graphman_cmd = ""
            if command == const.GRAPHMAN_REASSIGN:
                # Update decisionBasis to offchain before reassign for offchain subgraph
                if isOffchain == 1:
                    cmd_offchain = f"{config.indexer_graph} indexer rules set {ipfsHash} decisionBasis offchain --output=json --network={config.agent_network}"
                    result = subprocess.run([cmd_offchain], shell=True, check=True,
                                            stdout=subprocess.PIPE,
                                            universal_newlines=True)
                    output = result.stdout
                    print(output)
                    logging.info(cmd_offchain)
                    logging.info(output)

                graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} {command} {ipfsHash} {graphNode}"
            elif command == const.GRAPHMAN_UNASSIGN:
                # Update decisionBasis to never before unassign for offchain subgraph
                if isOffchain == 1:
                    update_decision_basic_never(ipfsHash)

                graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} {command} {ipfsHash}"
            elif command == const.GRAPHMAN_REMOVE:
                # Update decisionBasis to never before remove for offchain subgraph
                if isOffchain == 1:
                    update_decision_basic_never(ipfsHash)

                graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} drop --force  {ipfsHash}"
            elif command == const.GRAPHMAN_REWIND:
                block_hash = get_block_hash(ipfsHash, int(rewindBlock))
                if block_hash == -1:
                    return const.ERROR
                graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} {command} --block-hash {block_hash} --block-number {rewindBlock} {ipfsHash}"
            if len(graphman_cmd) > 0:
                logging.info("graphman_cmd: " + graphman_cmd)
                result = subprocess.run([graphman_cmd], shell=True, check=True,
                                        stdout=subprocess.PIPE,
                                        universal_newlines=True)
                output = result.stdout
                logging.info("output: " + str(output))
            return const.SUCCESS
        else:
            return const.TOKEN_ERROR
    except Exception as e:
        print(e)
        logging.error("graphman: " + str(e))
        return const.ERROR


def update_decision_basic_never(ipfsHash):
    cmd_offchain = f"{config.indexer_graph} indexer rules set {ipfsHash} decisionBasis never --network={config.agent_network}"
    result = subprocess.run([cmd_offchain], shell=True, check=True,
                            stdout=subprocess.PIPE,
                            universal_newlines=True)
    output = result.stdout
    logging.info(cmd_offchain)
    logging.info(output)


@app.route('/checkRPCs', methods=['POST'])
def check_rpc():
    try:
        token = request.form.get("token")
        if token != config.token:
            return const.TOKEN_ERROR

        if len(config.rpc_list) == 0:
            return "ERROR"

        listRpcCheckingResult = []
        # For erc20
        for rpc in config.rpc_list["erc20"]:
            block_number = -1
            chain_id = -1
            peer_count = -1
            try:
                logging.info("check rpc: " + rpc)
                playload_eth_blocknumber = {"method": "eth_blockNumber", "params": [], "id": 1, "jsonrpc": "2.0"}
                headers = {
                    "content-type": "application/json"
                }
                response = requests.post(url=rpc,
                                         json=playload_eth_blocknumber,
                                         headers=headers)
                if response.ok:
                    response_json = response.json()
                    block_number = int(response_json["result"], 16)
                playload_eth_chainid = {"method": "eth_chainId", "params": [], "id": 1, "jsonrpc": "2.0"}
                response = requests.post(url=rpc,
                                         json=playload_eth_chainid,
                                         headers=headers)
                if response.ok:
                    response_json = response.json()
                    chain_id = int(response_json["result"], 16)
                playload_peer = {"jsonrpc": "2.0", "method": "net_peerCount", "params": [], "id": 67}
                response = requests.post(url=rpc,
                                         json=playload_peer,
                                         headers=headers)
                if response.ok:
                    response_json = response.json()
                    peer_count = int(response_json["result"], 16)

                rpcInfo = {"rpc": rpc, "block_number": block_number, "chain_id": chain_id, "peer_count": peer_count}
                logging.info("check rpc result: " + json.dumps(rpcInfo))
                listRpcCheckingResult.append(rpcInfo)
            except Exception as e:
                print(e)
                logging.error("error checking rpc: " + str(e))
                rpcInfo = {"rpc": rpc, "block_number": block_number, "chain_id": chain_id, "peer_count": peer_count}
                listRpcCheckingResult.append(rpcInfo)
        return json.dumps(listRpcCheckingResult)
    except Exception as e:
        print(e)
        logging.error("check_rpc: " + str(e))
        return const.ERROR


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
        return const.ERROR


@app.route('/getGraft', methods=['GET'])
def get_graft_data():
    ipfs = request.args.get('ipfs')
    conn = sqlite3.connect('subgraph_database.db')
    cursor = conn.cursor()
    cursor.execute('''
    SELECT graft_ipfs,graft_block FROM manage_graft WHERE ipfs = ?
    ''', (ipfs,))
    rows = cursor.fetchall()
    conn.close()

    if rows:
        return jsonify({
            'ipfs': ipfs,
            'graft_ipfs': rows[0][0],
            'graft_block': rows[0][1]
        })
    else:
        return jsonify({'message': 'IPFS hash not found'}), 404


@app.route('/getAllGraft', methods=['GET'])
def get_all_grafts():
    conn = sqlite3.connect('subgraph_database.db')
    cursor = conn.cursor()
    cursor.execute('SELECT ipfs, graft_ipfs, graft_block FROM manage_graft')
    rows = cursor.fetchall()
    conn.close()

    all_data = []
    for row in rows:
        all_data.append({
            'ipfs': row[0],
            'graft_ipfs': row[1],
            'graft_block': row[2]
        })

    return jsonify(all_data)


if __name__ == '__main__':
    app.run(host=config.host, port=config.port, debug=True)
