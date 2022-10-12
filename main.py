import const
import json
import requests
import subprocess
import time

# Global variables
token = ""
network = ""
indexer_management_url = ""
indexer_graph = ""
# indexer_address = ""
# txn_rpc = ""
# graph_index_node = ""

headers = {'Content-Type': 'application/x-www-form-urlencoded'}


def print_hi():
    print(f'\n\n************* This is a tool that will help the indexer manage allocations on the website '
          f'*************\n\n')


def request_update_exe_status(id):
    params = {
        'id': id,
        'token': token,
        'network': network
    }
    response = requests.post(const.API_UPDATE_EXE_STATUS, data=params, headers=headers)
    if response.status_code == 200 and response.json()["code"] == 0:
        print("************ UPDATE EXE STATUS SUCCESS ***********\n")
    else:
        print("************ UPDATE EXE STATUS ERROR ***********\n")


def request_update_time(token):
    params = {
        'token': token,
        'timeUpdate': time.time()
    }
    response = requests.post(const.API_UPDATE_TIME, data=params, headers=headers)
    if response.status_code == 200 and response.json()["code"] == 0:
        print("************ UPDATE TIME SUCCESS ***********\n")
    else:
        print("************ UPDATE TIME ERROR ***********\n")


def request_actions(url):
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
                    if actionId == const.CLOSE_ACTION:
                        print(" ************************************* ")
                        print(" Start close this subgraph !")
                        if poi == 'NULL':
                            cmd_unallocate_action = f"{indexer_graph} indexer actions queue unallocate {ipfsHash} {allocateId} --output=json"
                        else:
                            cmd_unallocate_action = f"{indexer_graph} indexer actions queue unallocate {ipfsHash} {allocateId} {poi} true --output=json"

                        print("Execute cmd : " + cmd_unallocate_action)
                        process = subprocess.run([cmd_unallocate_action], shell=True, check=True,
                                                 stdout=subprocess.PIPE,
                                                 universal_newlines=True)
                        close_output = process.stdout
                        print("Close action status : " + close_output)
                    elif actionId == const.OPEN_ACTION:
                        print(" ************************************* ")
                        print(" Start open this subgraph !")
                        cmd_allocate_action = f"{indexer_graph} indexer actions queue allocate {ipfsHash} {allocatedTokens} --output=json"
                        print("Execute cmd : " + cmd_allocate_action)
                        process = subprocess.run([cmd_allocate_action], shell=True, check=True,
                                                 stdout=subprocess.PIPE,
                                                 universal_newlines=True)
                        close_output = process.stdout
                        print("OPEN action status : " + close_output)
                    elif actionId == const.RE_ALLOCATION_ACTION:
                        print(" ************************************* ")
                        print(" Start re-allocation this subgraph !")
                        if poi == 'NULL':
                            cmd_reallocate_action = f"{indexer_graph} indexer actions queue reallocate {ipfsHash} {allocateId} {allocatedTokens} --output=json"
                        else:
                            cmd_reallocate_action = f"{indexer_graph} indexer actions queue reallocate {ipfsHash} {allocateId} {allocatedTokens} {poi} true --output=json"

                        print("Execute cmd : " + cmd_reallocate_action)
                        process = subprocess.run([cmd_reallocate_action], shell=True, check=True,
                                                 stdout=subprocess.PIPE,
                                                 universal_newlines=True)
                        re_allocation_output = process.stdout
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
                print("Execute cmd : " + cmd_offchain)
                process = subprocess.run([cmd_offchain], shell=True, check=True,
                                         stdout=subprocess.PIPE,
                                         universal_newlines=True)
                offchain_output = process.stdout
                print("Offchain action status : " + offchain_output)

                # update exe status on server to avoid dupplicate action
                request_update_exe_status(serverId)
                request_update_time(token)
            if executeCount > 0:
                print(f"=====> Total actions need to approve : {executeCount}")
                cmd_approve_all_queue = f"{indexer_graph} indexer actions approve queued --output=json"
                process = subprocess.run([cmd_approve_all_queue], shell=True, check=True,
                                         stdout=subprocess.PIPE,
                                         universal_newlines=True)
                approve_output = process.stdout
                print("Approve all actions : " + approve_output)
        else:
            print("There is no action to execute !")


def verify_config():
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


def read_config_file():
    try:
        global token
        global network
        global indexer_management_url
        global indexer_graph
        config_file = open(".config", "r")
        config_obj = json.loads(config_file.read())
        token = config_obj["token"]
        network = config_obj["network"]
        indexer_management_url = config_obj["indexer_management_url"]
        indexer_graph = config_obj["indexer_graph"]
        # indexer_address = config_obj["indexer_address"]
        # txn_rpc = config_obj["txn_rpc"]
        # graph_index_node = config_obj["graph_index_node"]

        config_file.close()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    print_hi()
    read_config_file()
    verify_config()
    while True:
        try:
            request_actions(const.API_GET_CLOSE_ACTIONS)
            request_actions(const.API_GET_OPEN_ACTIONS)
        except Exception as e:
            print(e)

        time.sleep(20)
