import subprocess
import threading
import request_actions
import monitor_rpc
import manage_graft
import monitor_subgraph_syncing

if __name__ == '__main__':
    request_actions.print_hi()
    if request_actions.verify_config():
        thread_start_request_actions = threading.Thread(target=request_actions.start_request_actions, args=())
        thread_start_request_actions.start()

        thread_start_manager_graft = threading.Thread(target=manage_graft.start_manage_graft, args=())
        thread_start_manager_graft.start()

        thread_start_monitor_rpc = threading.Thread(target=monitor_rpc.start_monitor_rpc, args=())
        thread_start_monitor_rpc.start()

        thread_start_monitor_subgraph_syncing = threading.Thread(target=monitor_subgraph_syncing.monitor_subgraph_syncing, args=())
        thread_start_monitor_subgraph_syncing.start()

        cmd_start_manage_agent_action = f"python3 manage_actions.py"
        subprocess.run([cmd_start_manage_agent_action], shell=True, check=True)