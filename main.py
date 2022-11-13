import subprocess
import threading
import request_actions

if __name__ == '__main__':
    request_actions.print_hi()
    if request_actions.verify_config():
        thread_start_request_actions = threading.Thread(target=request_actions.start_request_actions, args=())
        thread_start_request_actions.start()

        cmd_start_manage_agent_action = f"python3 manage_actions.py"
        subprocess.run([cmd_start_manage_agent_action], shell=True, check=True)
