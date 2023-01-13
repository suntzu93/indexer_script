import time
import config
import requests
import logging
import const


def get_near_chain_head(rpc):
    try:
        response = requests.get(rpc)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the response JSON
            response_json = response.json()
            # Get the latest block number
            block_number = response_json["sync_info"]["latest_block_height"]
            return block_number

        return -1
    except Exception as e:
        print(e)
        logging.error("get_near_chain_head: " + str(rpc))
        return -1


def get_cosmos_chain_head(rpc):
    try:
        response = requests.get(rpc)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the response JSON
            response_json = response.json()
            # Get the latest block height
            chain_head = response_json["result"]["sync_info"]["latest_block_height"]
            return chain_head

        return -1
    except Exception as e:
        print(e)
        logging.error("get_cosmos_chain_head: " + str(rpc))
        return -1


def get_erc20_chain_head(rpc):
    try:
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
            return block_number
        return -1
    except Exception as e:
        print(e)
        logging.error("get_erc20_chain_head: " + str(rpc))
        return -1


def get_chain_rpc(chain_id):
    chain_ids = {
        1: const.ETH_RPC_ENDPOINT,
        5: const.GOERLI_RPC_ENDPOINT,
        10: const.OPTIMISM_RPC_ENDPOINT,
        56: const.BSC_RPC_ENDPOINT,
        99: const.POA_RPC_ENDPOINT,
        100: const.GNOSIS_RPC_ENDPOINT,
        122: const.FUSE_RPC_ENDPOINT,
        137: const.POLYGON_RPC_ENDPOINT,
        250: const.FANTOM_RPC_ENDPOINT,
        42161: const.ARBITRUM_RPC_ENDPOINT,
        42170: const.ARBITRUM_NOVA_RPC_ENDPOINT,
        42220: const.CELO_RPC_ENDPOINT,
        43114: const.AVAX_RPC_ENDPOINT,
        1313161554: const.AURORA_RPC_ENDPOINT,
        1666600000: const.HARMONY_RPC_ENDPOINT,
    }
    return chain_ids.get(chain_id, chain_id)


def monitor_rpc():
    try:
        # For erc20
        if "erc20" in config.rpc_list and config.rpc_list.get("erc20"):
            for your_rpc in config.rpc_list.get("erc20"):
                try:
                    logging.info("check rpc: " + your_rpc)
                    headers = {
                        "content-type": "application/json"
                    }
                    playload_eth_chainid = {"method": "eth_chainId", "params": [], "id": 1, "jsonrpc": "2.0"}
                    response = requests.post(url=your_rpc,
                                             json=playload_eth_chainid,
                                             headers=headers,
                                             timeout=5)
                    if response.ok:
                        response_json = response.json()
                        chain_id = int(response_json["result"], 16)
                        chain_rpc = get_chain_rpc(chain_id)
                        chain_head_block_number = get_erc20_chain_head(chain_rpc)
                        current_block_number = get_erc20_chain_head(your_rpc)
                        behind_block_number = chain_head_block_number - current_block_number
                        # check healthy of your rpc
                        check_healthy(behind_block_number, chain_head_block_number, chain_rpc, current_block_number,
                                      your_rpc)
                    else:
                        message = """Can not fetch chain_id from your rpc : %s""" % (
                            your_rpc)
                        send_alert_msg(message)
                except requests.exceptions.Timeout:
                    message = """Timeout to fetch data from your rpc : %s""" % (
                        your_rpc)
                    send_alert_msg(message)
                except Exception as e:
                    print(e)
                    logging.error("error checking rpc: " + str(e))
                    message = """Can not fetch data from your rpc : %s""" % (
                        your_rpc)
                    send_alert_msg(message)

        if "near" in config.rpc_list and config.rpc_list.get("near"):
            try:
                for your_rpc in config.rpc_list.get("near"):
                    chain_head_block_number = get_near_chain_head(const.NEAR_RPC_ENDPOINT)
                    current_block_number = get_near_chain_head(your_rpc)
                    behind_block_number = chain_head_block_number - current_block_number
                    # check healthy of your rpc
                    check_healthy(behind_block_number, chain_head_block_number, const.NEAR_RPC_ENDPOINT,
                                  current_block_number,
                                  your_rpc)
            except Exception as e:
                print(e)
                logging.error("error checking near rpc: " + str(e))

        if "cosmos" in config.rpc_list and config.rpc_list.get("cosmos"):
            try:
                for your_rpc in config.rpc_list.get("cosmos"):
                    chain_head_block_number = get_cosmos_chain_head(const.COSMOS_RPC_ENDPOINT)
                    current_block_number = get_cosmos_chain_head(your_rpc)
                    behind_block_number = int(chain_head_block_number) - int(current_block_number)
                    # check healthy of your rpc
                    check_healthy(behind_block_number, chain_head_block_number, const.COSMOS_RPC_ENDPOINT,
                                  current_block_number,
                                  your_rpc)
            except Exception as e:
                print(e)
                logging.error("error checking cosmos rpc: " + str(e))
    except Exception as e:
        print(e)
        logging.error("check_rpc: " + str(e))
        return const.ERROR


def check_healthy(behind_block_number, chain_head_block_number, chain_rpc, current_block_number, rpc):
    if chain_head_block_number != -1 and behind_block_number > config.threshold_block_behind:
        message = """Your chain %s has a problem. 
                                            \n-------------------
                                            \nYour current blockHeight : %s
                                            \nChainhead blockHeight : %s
                                            \nBlock behind: %s""" % (
            rpc, str(current_block_number), str(chain_head_block_number),
            str(behind_block_number))
        send_alert_msg(message)
    elif chain_head_block_number == -1:
        message = """Can not fetch blockHeight for public rpc : %s""" % (
            chain_rpc)
        send_alert_msg(message)
    else:
        print("Your chain rpc " + rpc + " is good !")


def send_alert_msg(message):
    try:
        params = {
            'chatId': config.chat_id,
            'msg': message,
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(const.API_ALERT_RPC, data=params, headers=headers)
        if response.status_code == 200 and response.json()["code"] == 0:
            print("************ SEND ALERT SUCCESS ***********\n")
            logging.info("alert: " + message)
        else:
            print("************ SEND ALERT ERROR ***********\n")
            logging.info("alert: " + message)
    except Exception as e:
        print(e)
        logging.error("alert_msg: " + str(e))


def start_monitor_rpc():
    try:
        if isinstance(config.chat_id, (int, float)) and config.chat_id != 0:
            print("Start monitor rpc !")
            while True:
                try:
                    monitor_rpc()
                    time.sleep(2 * 60)  # 2 minutes
                except Exception as e:
                    print(e)
                    logging.error("start_monitor_rpc:" + str(e))
        else:
            print("chat_id is not correct so stop monitor rpc !")

    except Exception as e:
        print(e)
        logging.error("start_monitor_rpc: " + str(e))

