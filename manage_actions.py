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
import pending_reward
import re
import database_size
import re
from datetime import datetime
from create_pub_sub import handle_create_pub_sub, handle_drop_pub_sub, compare_row_counts, get_publication_stats, get_subscription_stats, remove_schema_from_replica

app = Flask(__name__)
CORS(app)

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Create handlers for both file and console output
    handlers = [
        logging.FileHandler('manage_actions.log'),
        logging.StreamHandler()
    ]
    
    # Create formatter with timestamp
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Add formatter to handlers and handlers to logger
    for handler in handlers:
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

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
        'base': const.BASE_RPC_ENDPOINT,
        'scroll': const.SCROLL_RPC_ENDPOINT,
        'linea': const.LINEA_RPC_ENDPOINT,
        'blast': const.BLAST_RPC_ENDPOINT,
        'moonbeam': const.MOONBEAM_RPC_ENDPOINT,
        'sonic': const.SONIC_RPC_ENDPOINT,
    }
    return chain_rpcs.get(chain_name, None)


def get_block_hash(deployment, block_number, network):
    try:
        chain_rpc = get_chain_rpc(network)
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
        print("get block hash error : " + str(e))
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
        network = request.form.get("network")
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
            print("start request block broken: " + str(data_eth_getBlockByNumber))
            response = requests.post(url=const.ARBITRUM_RPC_ENDPOINT,
                                     headers=headers,
                                     data=json.dumps(data_eth_getBlockByNumber))
            print("response block broken: " + str(response.json()))
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
        print("json_data: " + str(json_data))

        if len(json_data["data"]["epoches"]) == 0 and blockBroken > 16568309:
            graphql_startBlock = """
                            {
                              epoches(first:1 ,where: {startBlock_lt: %s},orderBy: startBlock,orderDirection:desc) {
                                startBlock
                              }
                            }
                            """
            response = requests.post(url=config.indexer_agent_network_subgraph_endpoint,
                                     json={"query": graphql_startBlock % (blockBroken)})
            json_data = response.json()
            print("data epoches: " + str(json_data))

        if response.status_code == 200:
            if len(json_data['data']['epoches']) == 0 and 16083151 < blockBroken < 16568309:
                startBlock = 16083151
            else:
                startBlock = json_data["data"]["epoches"][0]["startBlock"]
            block_hash = get_block_hash(deployment, startBlock, network)

            proof_of_indexing = '{proofOfIndexing(subgraph:"%s",blockHash:"%s",blockNumber:%s,indexer:"%s")}' % (
                deployment, block_hash, startBlock, config.indexer_address)
            print("proof_of_indexing: " + proof_of_indexing)
            graphql_proof_of_indexing = {
                "query": proof_of_indexing
            }

            poi_response = requests.post(url=config.indexer_node_rpc,
                                         json=graphql_proof_of_indexing)
            print("poi_response: " + str(poi_response.json()))
            if poi_response.status_code == 200:
                json_poi = poi_response.json()
                print("json_poi: " + str(json_poi))
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
            if subgraphs and subgraphs != "all":
                graphql_healthy_subgraph = """
                                            { indexingStatuses(subgraphs: [%s]) { subgraph paused synced health node fatalError {message deterministic block { number }} chains {network latestBlock {number} chainHeadBlock {number} earliestBlock{number}}}}""" % subgraphs
            else:
                graphql_healthy_subgraph = "{ indexingStatuses { subgraph paused synced health node fatalError {message deterministic block { number }} chains {network latestBlock {number} chainHeadBlock {number} earliestBlock{number}}}}"

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


@app.route('/getAllFees', methods=['POST'])
def get_all_fees():
    try:
        token = request.form.get("token")
        if token != config.token:
            return const.TOKEN_ERROR

        fees = pending_reward.get_total_pending_reward()
        return jsonify({
            "status": "success",
            "data": fees
        }), 200
    except Exception as e:
        logging.error(f"Error in get_all_fees: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/getQueryFees', methods=['POST'])
def get_allocation_reward():
    try:
        token = request.form.get("token")
        allocateId = request.form.get("allocateId")
        if token != config.token:
            return const.TOKEN_ERROR

        fees = pending_reward.get_allocation_reward(allocateId)
        return jsonify({
            "status": "success",
            "data": fees
        }), 200
    except Exception as e:
        logging.error(f"Error in get_all_fees: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

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
        isOffchain = int(request.form.get("isOffchain", 0))
        network = request.form.get("networkId")
        name = request.form.get("name")
        
        if token != config.token:
            return const.TOKEN_ERROR

        logging.info(f"{command or ''} {ipfsHash or ''} {str(graphNode) or ''} {str(rewindBlock) or ''} {network or ''} {name or ''}")
        
        graphman_cmd = ""
        
        if command == const.GRAPHMAN_REASSIGN:
            # Update decisionBasis to offchain before reassign for offchain subgraph
            if isOffchain == 1:
                try:
                    cmd_offchain = f"{config.indexer_graph} indexer rules set {ipfsHash} decisionBasis offchain --output=json --network={config.agent_network}"
                    result = subprocess.run([cmd_offchain], shell=True, check=True,
                                            stdout=subprocess.PIPE,
                                            universal_newlines=True)
                    output = result.stdout
                    print(output)
                    logging.info(cmd_offchain)
                    logging.info(output)
                except subprocess.CalledProcessError as e:
                    logging.error(f"Error executing offchain command: {e}")
                    return const.ERROR

            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} {command} {ipfsHash} {graphNode}"
        elif command == const.GRAPHMAN_UNASSIGN:
            update_decision_basic_never(ipfsHash)

            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} reassign {ipfsHash} removed"
        elif command == const.GRAPHMAN_REMOVE:
            update_decision_basic_never(ipfsHash)

            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} drop --force  {ipfsHash}"
        elif command == const.GRAPHMAN_REWIND:
            block_hash = get_block_hash(ipfsHash, int(rewindBlock), network)
            if block_hash == -1:
                return const.ERROR
            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} {command} --block-hash {block_hash} --block-number {rewindBlock} {ipfsHash}"
        elif command == const.GRAPHMAN_PAUSE:
            update_decision_basic_never(ipfsHash)

            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} pause {ipfsHash}"
        elif command == const.GRAPHMAN_RESUME:
            try:
                cmd_offchain = f"{config.indexer_graph} indexer rules set {ipfsHash} decisionBasis offchain --output=json --network={config.agent_network}"
                result = subprocess.run([cmd_offchain], shell=True, check=True,
                                        stdout=subprocess.PIPE,
                                        universal_newlines=True)
                output = result.stdout
                logging.info(cmd_offchain)
                logging.info(output)
            except subprocess.CalledProcessError as e:
                logging.error(f"Error executing resume command: {e}")
                return const.ERROR

            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} resume {ipfsHash}"
        elif command == const.GRAPHMAN_INFO:
            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} info {ipfsHash}"
        elif command == const.GRAPHMAN_SUBGRAPH_CREATE_DEPLOY:
            if not name or not ipfsHash:
                return jsonify({"status": "error", "message": "Both name and ipfsHash are required for subgraph_create_deploy"}), 400
            
            # Step 1: Create subgraph
            create_cmd = f"{config.graphman_cli} -c {config.graphman_config_file} create {name}"
            logging.info("Create command: " + create_cmd)
            try:
                create_result = subprocess.run([create_cmd], shell=True, check=True,
                                               stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE,
                                               universal_newlines=True)
                create_output = create_result.stdout
                create_error = create_result.stderr
                logging.info("Create output: " + str(create_output))
                logging.info("Create error: " + str(create_error))
            except subprocess.CalledProcessError as e:
                logging.error(f"Error executing create command: {e}")
                return jsonify({"status": "error", "message": str(e)}), 500

            # Step 2: Deploy subgraph
            deploy_cmd = f"{config.graphman_cli} -c {config.graphman_config_file} deploy --url={config.admin_node_rpc} {name} {ipfsHash}"
            logging.info("Deploy command: " + deploy_cmd)
            try:
                deploy_result = subprocess.run([deploy_cmd], shell=True, check=True,
                                               stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE,
                                               universal_newlines=True)
                deploy_output = deploy_result.stdout
                deploy_error = deploy_result.stderr
                logging.info("Deploy output: " + str(deploy_output))
                logging.info("Deploy error: " + str(deploy_error))

                # Combine outputs
                combined_output = f"Create output:\n{create_output}\n\nDeploy output:\n{deploy_output}"
                return jsonify({"status": "success", "output": combined_output})
            except subprocess.CalledProcessError as e:
                logging.error(f"Error executing deploy command: {e}")
                return jsonify({"status": "error", "message": str(e)}), 500
        elif command == const.GRAPHMAN_UNUSED:
            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} unused remove"
        
        if len(graphman_cmd) > 0:
            logging.info("graphman_cmd: " + graphman_cmd)
            try:
                result = subprocess.run([graphman_cmd], shell=True, check=True,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        universal_newlines=True)
                output = result.stdout
                error = result.stderr
                logging.info("output: " + str(output))
                logging.info("error: " + str(error))
                
                if command == const.GRAPHMAN_INFO:
                    # Parse the output for info command
                    info_data = []
                    current_info = {}
                    for line in output.strip().split('\n'):
                        if line.startswith('----------'):
                            if current_info:
                                info_data.append(current_info)
                                current_info = {}
                        else:
                            key, value = line.split('|')
                            current_info[key.strip()] = value.strip()
                    if current_info:
                        info_data.append(current_info)
                    
                    return jsonify({
                        "status": "success",
                        "data": info_data
                    })
                else:
                    return const.SUCCESS
            except subprocess.CalledProcessError as e:
                logging.error(f"Error executing graphman command: {e}")
                return const.ERROR
        else:
            return const.ERROR
    except Exception as e:
        logging.error(f"Unexpected error in graphman: {str(e)}")
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


@app.route('/getSubgraphSize', methods=['POST'])
def get_subgraph_size():
    try:
        token = request.form.get("token")
        if token != config.token:
            return const.TOKEN_ERROR

        subgraph_sizes = database_size.get_subgraph_sizes()
        if subgraph_sizes is None:
            return jsonify({"error": "Failed to retrieve subgraph sizes"}), 500

        return jsonify(subgraph_sizes)
    except Exception as e:
        logging.error(f"Error in get_subgraph_size: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/graphmanStats', methods=['POST'])
def graphman_stats():
    try:
        token = request.form.get("token")
        command = request.form.get("command")
        ipfsHash = request.form.get("ipfsHash")
        tableName = request.form.get("tableName")
        isClear = request.form.get("isClear", "").lower() == "true"
        
        if token != config.token:
            return const.TOKEN_ERROR

        logging.info(f"{command or ''} {ipfsHash or ''} {tableName or ''} {isClear}")
        
        graphman_cmd = ""
        
        if command == const.GRAPHMAN_STATS_ACCOUNT_LIKE:
            clear_option = "-c" if isClear else ""
            table_list = tableName.split(',')
            graphman_cmd = ""
            for table in table_list:
                graphman_cmd += f"{config.graphman_cli} --config {config.graphman_config_file} stats account-like {ipfsHash} {table.strip()} {clear_option}; "
                subprocess.run([graphman_cmd], shell=True, check=True,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        universal_newlines=True)
            
            return const.SUCCESS
        elif command == const.GRAPHMAN_STATS_SHOW:
            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} stats show {ipfsHash}"
            logging.info("graphman_cmd: " + graphman_cmd)
            try:
                result = subprocess.run([graphman_cmd], shell=True, check=True,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        universal_newlines=True)
                output = result.stdout
                error = result.stderr
                logging.info("output: " + str(output))
                logging.info("error: " + str(error))
                

                # Parse the output for stats show command
                lines = output.strip().split('\n')
                data = []
                    
                for line in lines[2:]:  # Skip the header lines
                    if line.startswith('  (a):'):
                        break
                    if line.strip():
                        parts = line.split('|')
                        if len(parts) == 4:
                            table_info = parts[0].strip().rsplit(None, 1)
                            table_name = table_info[0].strip()
                            account_like = '(a)' in table_info
                            
                            entities = int(parts[1].strip().replace(',', '') or 0)
                            versions = int(parts[2].strip().replace(',', '') or 0)
                            ratio = float(parts[3].strip().rstrip('%') or 0)
                            
                            data.append({
                                "table": table_name,
                                "entities": entities,
                                "versions": versions,
                                "ratio": ratio,
                                "account_like": account_like
                            })
                
                return jsonify({
                    "status": "success", 
                    "data": data
                })
            except subprocess.CalledProcessError as e:
                logging.error(f"Error executing graphman command: {e}")
                return jsonify({"status": "error", "message": str(e)}), 500
        else:
            return jsonify({"status": "error", "message": "Invalid command"}), 400
    except Exception as e:
        logging.error(f"Unexpected error in graphman_stats: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/getShards', methods=['POST'])
def get_shards():
    try:
        token = request.form.get("token")
        if token != config.token:
            return const.TOKEN_ERROR

        if hasattr(config, 'shards') and isinstance(config.shards, list):
            return jsonify(config.shards)
        else:
            # Return a default array with "primary" shard
            default_shards = ["primary"]
            return jsonify(default_shards)
    except Exception as e:
        logging.error(f"Error in get_shards: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/graphmanCopy', methods=['POST'])
def graphman_copy():
    try:
        token = request.form.get("token")
        if token != config.token:
            return const.TOKEN_ERROR

        command = request.form.get("command")

        if command == "list":
            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} copy list"
            
            result = subprocess.run([graphman_cmd], shell=True, check=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
            
            output = result.stdout
            
            # Parse the output
            copy_actions = []
            for block in output.split('-' * 78)[1:]:  # Split by separator lines
                lines = block.strip().split('\n')
                if len(lines) >= 3:  # Changed from 4 to 3 to include queued actions
                    action = {}
                    for line in lines:
                        key, value = line.split('|')
                        key = key.strip()
                        value = value.strip()
                        
                        if key == 'deployment':
                            action['deployment'] = value
                        elif key == 'action':
                            match = re.match(r'(\w+) -> (\w+) \((\w+)\)', value)
                            if match:
                                action['from'] = match.group(1)
                                action['to'] = match.group(2)
                                action['shard'] = match.group(3)
                        elif key in ['started', 'queued']:
                            action['status'] = key
                            action['timestamp'] = datetime.fromisoformat(value).isoformat()
                        elif key == 'progress':
                            match = re.match(r'([\d.]+)% done, (\d+)/(\d+)', value)
                            if match:
                                action['progress'] = float(match.group(1))
                                action['current'] = int(match.group(2))
                                action['total'] = int(match.group(3))
                    
                    copy_actions.append(action)
            
            return jsonify({
                "status": "success",
                "data": copy_actions
            })

        elif command == "copy":
            srcData = request.form.get("srcData")
            toShard = request.form.get("toShard")
            toNode = request.form.get("toNode")
            isReplace = request.form.get("isReplace", "false").lower() == "true"

            if not all([srcData, toShard, toNode]):
                return jsonify({"status": "error", "message": "Missing required parameters"}), 400

            flag = "--replace" if isReplace else "--activate"
            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} copy create {flag} {srcData} {toShard} {toNode}"
            
            result = subprocess.run([graphman_cmd], shell=True, check=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
            
            output = result.stdout.strip()
            
            return jsonify({
                "status": "success",
                "message": output
            })

        elif command == "cancel":
            ipfsHash = request.form.get("ipfsHash")
            if not ipfsHash:
                return jsonify({"status": "error", "message": "ipfsHash is required for cancel command"}), 400

            graphman_cmd = f"{config.graphman_cli} --config {config.graphman_config_file} unassign {ipfsHash}"
            
            result = subprocess.run([graphman_cmd], shell=True, check=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
            
            output = result.stdout.strip()
            
            return jsonify({
                "status": "success",
                "message": f"Copy action for {ipfsHash} has been cancelled. Output: {output}"
            })

        else:
            return jsonify({"status": "error", "message": "Invalid command"}), 400
    
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing graphman command: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    except Exception as e:
        logging.error(f"Unexpected error in graphman_copy: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/createPubAndSub', methods=['POST'])
def create_pub_and_sub():
    try:
        token = request.form.get("token")
        schema_name = request.form.get("schema_name")
        isCopy = request.form.get("isCopy", "true").lower() == "true"
        
        if token != config.token:
            return const.TOKEN_ERROR, 403

        if not schema_name:
            return jsonify({"status": "error", "message": "schema_name is required"}), 400

        result = handle_create_pub_sub(schema_name, isCopy)
        if result["status"] == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 500
    except Exception as e:
        logging.error(f"createPubAndSub: {str(e)}")
        return const.ERROR, 500

@app.route('/dropPubAndSub', methods=['POST'])
def drop_pub_and_sub():
    try:
        token = request.form.get("token")
        schema_name = request.form.get("schema_name")

        if token != config.token:
            return const.TOKEN_ERROR, 403

        if not schema_name:
            return jsonify({"status": "error", "message": "schema_name is required"}), 400

        result = handle_drop_pub_sub(schema_name)
        if result["status"] == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 500
    except Exception as e:
        logging.error(f"dropPubAndSub: {str(e)}")
        return const.ERROR, 500

@app.route('/compareRowCounts', methods=['POST'])
def compare_row_counts_api():
    try:
        token = request.form.get("token")
        schema_name = request.form.get("schema_name")
        use_exact_count = request.form.get("use_exact_count", "false").lower() == "true"

        if token != config.token:
            return const.TOKEN_ERROR, 403

        if not schema_name:
            return jsonify({"status": "error", "message": "schema_name is required"}), 400

        result = compare_row_counts(schema_name, use_exact_count)
        if result["status"] == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 500
    except Exception as e:
        logging.error(f"compareRowCounts: {str(e)}")
        return const.ERROR, 500

@app.route('/getPublicationStats', methods=['POST'])
def get_publication_stats_api():
    try:
        token = request.form.get("token")

        if token != config.token:
            return const.TOKEN_ERROR, 403

        result = get_publication_stats()
        if result["status"] == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 500
    except Exception as e:
        logging.error(f"getPublicationStats: {str(e)}")
        return const.ERROR, 500

@app.route('/getSubscriptionStats', methods=['POST'])
def get_subscription_stats_api():
    try:
        token = request.form.get("token")

        if token != config.token:
            return const.TOKEN_ERROR, 403

        result = get_subscription_stats()
        if result["status"] == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 500
    except Exception as e:
        logging.error(f"getSubscriptionStats: {str(e)}")
        return const.ERROR, 500

@app.route('/removeSchemaFromReplica', methods=['POST'])
def remove_schema_from_replica_api():
    try:
        token = request.form.get("token")
        schema_name = request.form.get("schema_name")

        if token != config.token:
            return const.TOKEN_ERROR, 403

        if not schema_name:
            return jsonify({"status": "error", "message": "schema_name is required"}), 400

        result = remove_schema_from_replica(schema_name)
        if result["status"] == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 500
    except Exception as e:
        logging.error(f"removeSchemaFromReplica: {str(e)}")
        return const.ERROR, 500

if __name__ == '__main__':
    setup_logging()
    app.run(host=config.host, port=config.port, debug=True)
