from flask import Flask
from flask import request
import subprocess
import config
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


def verify_config():
    cmd_connect_indexer_manager = f"{config.indexer_graph} indexer connect {config.indexer_management_url}"
    print("Start connect indexer manager: " + cmd_connect_indexer_manager)
    process = subprocess.run([cmd_connect_indexer_manager], shell=True, check=True, stdout=subprocess.PIPE,
                             universal_newlines=True)
    connect_indexer_manager_output = process.stdout
    if connect_indexer_manager_output.startswith(
            f"Indexer management API URL configured as \"{config.indexer_management_url}\""):
        print("** Connected success to indexer manager ! **")

    cmd_get_graph_vs = f"{config.indexer_graph} --version"
    process = subprocess.run([cmd_get_graph_vs], shell=True, check=True, stdout=subprocess.PIPE,
                             universal_newlines=True)
    graph_version = process.stdout
    print("===> Indexer graph version : " + graph_version)


verify_config()


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
