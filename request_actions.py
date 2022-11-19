import const
import json
import requests
import subprocess
import time
import config
import logging

# Global variables
token = config.token
network = config.network
indexer_management_url = config.indexer_management_url
indexer_graph = config.indexer_graph

headers = {'Content-Type': 'application/x-www-form-urlencoded'}


def print_hi():
    print(f'\n\n************* This is a tool that will help the indexer manage allocations on the website '
          f'*************')
    print(f'Starting indexer_script version ' + const.VERSION + "\n\n")

    logging.basicConfig(filename='indexer_script.log', level=logging.INFO)
    logging.info("version: " + const.VERSION)


def request_update_exe_status(id):
    try:
        params = {
            'id': id,
            'token': token,
            'network': network
        }
        response = requests.post(const.API_UPDATE_EXE_STATUS, data=params, headers=headers)
        if response.status_code == 200 and response.json()["code"] == 0:
            print("************ UPDATE EXE STATUS SUCCESS ***********\n")
            logging.info("UPDATE EXE STATUS SUCCESS " + str(id))
        else:
            print("************ UPDATE EXE STATUS ERROR ***********\n")
            logging.info("UPDATE EXE STATUS SUCCESS " + str(id) + ":" + response.status_code)
            logging.info("UPDATE EXE STATUS SUCCESS " + str(id) + ":" + str(response.json()["code"]))
    except Exception as e:
        print(str(e))
        logging.error("request_update_exe_status:" + str(e))


def request_update_time(token):
    try:
        params = {
            'token': token,
            'timeUpdate': time.time()
        }
        response = requests.post(const.API_UPDATE_TIME, data=params, headers=headers)
        if response.status_code == 200 and response.json()["code"] == 0:
            print("************ UPDATE TIME SUCCESS ***********\n")
            logging.info("UPDATE TIME SUCCESS ")
        else:
            print("************ UPDATE TIME ERROR ***********\n")
            logging.info("UPDATE TIME ERROR " + str(response.status_code))
            logging.info("UPDATE TIME ERROR " + str(response.json()["code"]))
    except Exception as e:
        print(str(e))
        logging.error("request_update_time:" + str(e))


def request_actions(url):
    try:
        params = {
            'token': token,
            'network': network,
        }

        response = requests.post(url, data=params, headers=headers)
        if response.status_code == 200:
            print(" ======================================================")
            print("Request actions success , here is the list actions : \n")
            actions_json = response.json()
            responseCode = actions_json["code"]
            actions_data = actions_json["data"]
            executeCount = 0
            if responseCode == 0 and len(actions_data) > 0:
                for action in actions_data:
                    try:
                        print("------------------------------")
                        serverId = action["id"]
                        ipfsHash = action["ipfsHash"]
                        status = action["status"]
                        networkId = action["networkId"]
                        allocatedTokens = action["allocatedTokens"]
                        actionId = action["action"]

                        if actionId == const.CLOSE_ACTION or actionId == const.RE_ALLOCATION_ACTION:
                            actionMsg = "CLOSE"
                            allocateId = action["allocateId"]
                            poi = action["poi"]
                            print("Valid POI    : ", poi)
                            print("Allocate Id  : ", allocateId)
                        else:
                            actionMsg = "OPEN"

                        print("Deployment   : ", ipfsHash)
                        print("Status       : ", status)
                        print("Network      : ", networkId)
                        print("Allocated    : ", allocatedTokens)
                        print("Action       : ", actionMsg)

                        if status == const.STATUS_APPROVE:
                            executeCount = executeCount + 1
                            logging.info(ipfsHash + " : " + str(serverId))
                            if actionId == const.CLOSE_ACTION:
                                print(" ************************************* ")
                                print(" Start close this subgraph !")
                                if poi == 'NULL':
                                    cmd_unallocate_action = f"{indexer_graph} indexer actions queue unallocate {ipfsHash} {allocateId} --output=json"
                                else:
                                    cmd_unallocate_action = f"{indexer_graph} indexer actions queue unallocate {ipfsHash} {allocateId} {poi} true --output=json"
                                logging.info(cmd_unallocate_action)

                                print("Execute cmd : " + cmd_unallocate_action)
                                process = subprocess.run([cmd_unallocate_action], shell=True, check=True,
                                                         stdout=subprocess.PIPE,
                                                         universal_newlines=True)
                                close_output = process.stdout
                                logging.info("unallocate success for deployment : " + ipfsHash)
                                print("Close action status : " + close_output)
                            elif actionId == const.OPEN_ACTION:
                                print(" ************************************* ")
                                print(" Start open this subgraph !")
                                cmd_allocate_action = f"{indexer_graph} indexer actions queue allocate {ipfsHash} {allocatedTokens} --output=json"
                                logging.info(cmd_allocate_action)
                                print("Execute cmd : " + cmd_allocate_action)
                                process = subprocess.run([cmd_allocate_action], shell=True, check=True,
                                                         stdout=subprocess.PIPE,
                                                         universal_newlines=True)
                                close_output = process.stdout

                                logging.info("allocate success for deployment : " + ipfsHash)
                                print("OPEN action status : " + close_output)
                            elif actionId == const.RE_ALLOCATION_ACTION:
                                print(" ************************************* ")
                                print(" Start re-allocation this subgraph !")
                                if poi == 'NULL':
                                    cmd_reallocate_action = f"{indexer_graph} indexer actions queue reallocate {ipfsHash} {allocateId} {allocatedTokens} --output=json"
                                else:
                                    cmd_reallocate_action = f"{indexer_graph} indexer actions queue reallocate {ipfsHash} {allocateId} {allocatedTokens} {poi} true --output=json"
                                logging.info(cmd_reallocate_action)

                                print("Execute cmd : " + cmd_reallocate_action)
                                process = subprocess.run([cmd_reallocate_action], shell=True, check=True,
                                                         stdout=subprocess.PIPE,
                                                         universal_newlines=True)
                                re_allocation_output = process.stdout
                                logging.info("reallocate success for deployment : " + ipfsHash)
                                print("Close action status : " + re_allocation_output)
                            # update exe status on server to avoid dupplicate action
                            request_update_exe_status(serverId)
                            request_update_time(token)
                        else:
                            print("====> Status is Queue -> No execute !")

                        # For offchain dont need approve
                        if actionId == const.OFFCHAIN_ACTION:
                            print(" ************************************* ")
                            print(" Start sync offchain this subgraph !")
                            cmd_offchain = f"{indexer_graph} indexer rules set {ipfsHash} decisionBasis offchain --output=json"
                            logging.info(cmd_offchain)

                            print("Execute cmd : " + cmd_offchain)
                            process = subprocess.run([cmd_offchain], shell=True, check=True,
                                                     stdout=subprocess.PIPE,
                                                     universal_newlines=True)
                            offchain_output = process.stdout
                            print("Offchain action status : " + offchain_output)
                            logging.info("Offchain success for deployment : " + ipfsHash)
                            # update exe status on server to avoid dupplicate action
                            request_update_exe_status(serverId)
                            request_update_time(token)
                    except Exception as e:
                        print(e)
                        logging.error("for actions_data: " + str(e))
                        request_update_exe_status(serverId)
                        request_update_time(token)
                    except subprocess.CalledProcessError as subError:
                        print(subError)
                        request_update_exe_status(serverId)
                        request_update_time(token)
                        logging.error(
                            "command '{}' return with error (code {}): {}".format(subError.cmd, subError.returncode,
                                                                                  subError.output))
                # end for

                if executeCount > 0:
                    try:
                        print(f"=====> Total actions need to approve : {executeCount}")
                        cmd_approve_all_queue = f"{indexer_graph} indexer actions approve queued --output=json"
                        logging.info(cmd_approve_all_queue)

                        process = subprocess.run([cmd_approve_all_queue], shell=True, check=True,
                                                 stdout=subprocess.PIPE,
                                                 universal_newlines=True)
                        approve_output = process.stdout
                        logging.info("approve success for all queued actions !")
                        print("Approve all actions : " + approve_output)
                    except subprocess.CalledProcessError as subError:
                        print(subError)
                        logging.error("Approve queue exception : " + subError)
            else:
                print("There is no action to execute !")
    except Exception as e:
        print(str(e))
        logging.error("request_actions:" + str(e))


def verify_config():
    try:
        cmd_connect_indexer_manager = f"{indexer_graph} indexer connect {indexer_management_url}"
        print("Start connect indexer manager: " + cmd_connect_indexer_manager)
        process = subprocess.run([cmd_connect_indexer_manager], shell=True, check=True, stdout=subprocess.PIPE,
                                 universal_newlines=True)
        connect_indexer_manager_output = process.stdout
        if connect_indexer_manager_output.startswith(
                f"Indexer management API URL configured as \"{indexer_management_url}\""):
            print("** Connected success to indexer manager ! **")

        cmd_get_graph_vs = f"{indexer_graph} --version"
        process = subprocess.run([cmd_get_graph_vs], shell=True, check=True, stdout=subprocess.PIPE,
                                 universal_newlines=True)
        graph_version = process.stdout
        print("Indexer graph version : " + graph_version)
        return True
    except Exception as e:
        print(e)
        logging.error(str(e))
        return False


def start_request_actions():
    while True:
        try:
            request_actions(const.API_GET_CLOSE_ACTIONS)
            request_actions(const.API_GET_OPEN_ACTIONS)
        except Exception as e:
            print(e)
            logging.error("start_request_actions:" + str(e))

        time.sleep(20)
