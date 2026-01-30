import os
import glob
import time
import subprocess
import uuid
import logging
import smtplib
import re
import random
import select
import sys
import json
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

# ==========================================
# 🔧 USER CONFIGURATION
# ==========================================
PROJECT_ID = "stoked-cosine-415611"
ZONE = "us-central1-c"
K8SCLUSTER_NAME = "bcgossip-cluster"
K8SNODE_COUNT = 5  

IMAGE_NAME = "wwiras/simcl2"
IMAGE_TAG = "v14"
TOPOLOGY_FOLDER = "topology"
HELM_CHART_FOLDER = "simcl2" 

EXPERIMENT_DURATION = 20    
BASE_TRIGGER_TIMEOUT = 10   # Starting timeout
TIMEOUT_INCREMENT = 2       # Add 2 seconds for every subsequent test cycle
NUM_REPEAT_TESTS = 3        # Number of cycles per topology

# ==========================================
# 📝 LOGGING SETUP
# ==========================================
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"orchestrator_{timestamp_str}.log"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()]
)

def log(msg):
    logging.info(msg)

# ==========================================
# 🛠️ HELPER CLASS
# ==========================================
class ExperimentHelper:
    def __init__(self):
        pass

    def run_command(self, command, shell=True, suppress_output=False):
        try:
            cmd_str = command if isinstance(command, str) else " ".join(command)
            result = subprocess.run(command, check=True, text=True, capture_output=True, shell=shell)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            if not suppress_output:
                log(f"❌ Error executing: {e.cmd}\nStderr: {e.stderr}")
            raise e

    def wait_for_pods_to_be_ready(self, namespace='default', expected_pods=0, timeout=600):
        log(f"⏳ Waiting for {expected_pods} pods to be ready...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                cmd = f"kubectl get pods -n {namespace} --no-headers | grep Running | wc -l"
                running_pods = int(self.run_command(cmd))
                
                if running_pods >= expected_pods:
                    log(f"✅ All {running_pods} pods are READY.")
                    return True
                else:
                    if int(time.time()) % 10 == 0:
                        log(f"   ... {running_pods}/{expected_pods} pods ready.")
            except Exception as e:
                log(f"⚠️ Error checking pods: {e}")
            time.sleep(2)
        return False

    def select_random_pod(self):
        cmd = "kubectl get pods --no-headers | grep Running | awk '{print $1}'"
        stdout = self.run_command(cmd)
        pod_list = stdout.split()
        if not pod_list: raise Exception("No running pods found.")
        return random.choice(pod_list)

    def trigger_gossip_hybrid(self, pod_name, test_id, cycle_index):
        """
        HYBRID STRATEGY UPDATED:
        1. Dynamic Timeout: Scales with cycle_index
        2. Strict ID Match: Acks must contain the test_id
        """
        # A. Calculate Dynamic Timeout
        # Cycle 1: 10s + (0*2) = 10s
        # Cycle 5: 10s + (4*2) = 18s
        current_timeout = BASE_TRIGGER_TIMEOUT + ((cycle_index - 1) * TIMEOUT_INCREMENT)
        
        message = f"{test_id}-trigger"
        log(f"⚡ Triggering Gossip in {pod_name} (Msg: {message})")
        log(f"   🛑 Timeout set to {current_timeout}s (Cycle {cycle_index})")
        
        cmd = [
            'kubectl', 'exec', pod_name, '--', 
            'python3', 'start.py', '--message', message
        ]
        
        start_time = time.time()
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True, 
            bufsize=1 
        )

        ack_received = False

        while True:
            # 1. CHECK DYNAMIC TIMEOUT
            if time.time() - start_time > current_timeout:
                log(f"⏱️ Trigger Timeout ({current_timeout}s) reached. Moving on...")
                process.kill()
                return False 

            # 2. READ OUTPUT
            reads = [process.stdout.fileno()]
            ready = select.select(reads, [], [], 1.0)[0]

            if ready:
                line = process.stdout.readline()
                if not line: break 
                
                line = line.strip()
                # log(f"   [Pod]: {line}") # Debugging

                # 3. STRICT ID MATCHING (Point B)
                # We check for "acknowledgment" AND the specific "test_id"
                # This ensures we don't catch a delayed ACK from a previous run
                if "Received acknowledgment" in line:
                    if test_id in line:
                        log(f"✅ VALID ACK RECEIVED for {test_id}! (Latency: {time.time() - start_time:.2f}s)")
                        ack_received = True
                        process.terminate() 
                        return True
                    else:
                        log(f"⚠️ Ignored stale/mismatched ACK in line: {line}")
            
            if process.poll() is not None:
                break

        if ack_received:
            return True
        else:
            log("⚠️ Trigger process ended without valid ACK.")
            return False

# ==========================================
# 🚀 MAIN ORCHESTRATOR
# ==========================================
def main():
    helper = ExperimentHelper()
    ROOT_DIR = os.getcwd() 
    
    files = glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json"))
    files.sort()
    if not files: 
        log("❌ No topology files found.")
        return

    # 1. INFRASTRUCTURE
    log(f"🔨 Ensuring Cluster {K8SCLUSTER_NAME}...")
    try:
        subprocess.run([
            "gcloud", "container", "clusters", "create", K8SCLUSTER_NAME,
            "--zone", ZONE, "--num-nodes", str(K8SNODE_COUNT), "--machine-type", "e2-medium", "--quiet"
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except: pass
    
    subprocess.run([
        "gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME,
        "--zone", ZONE, "--project", PROJECT_ID
    ], check=True)

    try:
        # 2. LOOP FILES
        for i, filepath in enumerate(files):
            filename = os.path.basename(filepath)
            
            p2p_nodes = 0
            node_match = re.search(r"nodes(\d+)", filename)
            if node_match: p2p_nodes = int(node_match.group(1))

            log(f"\n[{i+1}/{len(files)}] 🚀 STARTING TOPOLOGY: {filename}")

            try:
                # --- A. DEPLOY HELM ---
                log(f"📂 Installing Helm Chart ({p2p_nodes} pods)...")
                os.chdir(HELM_CHART_FOLDER)
                try:
                    helm_cmd = (
                        f"helm install simcn ./chartsim "
                        f"--set testType=default,totalNodes={p2p_nodes},"
                        f"image.tag={IMAGE_TAG},image.name={IMAGE_NAME}"
                    )
                    helper.run_command(helm_cmd)
                finally:
                    os.chdir(ROOT_DIR)

                # --- B. WAIT FOR PODS ---
                if not helper.wait_for_pods_to_be_ready(expected_pods=p2p_nodes):
                    raise Exception("Pods never became ready")

                # --- C. INJECT TOPOLOGY ---
                log(f"💉 Injecting Topology: {filename}")
                # res = subprocess.run(f"python3 prepare.py --filename {filename}", shell=True)
                # if res.returncode != 0: raise Exception("Topology injection failed")

                # --- D. REPEAT TEST LOOP ---
                for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                    
                    # Create Unique ID
                    run_id = f"{filename.replace('.json', '')}_Run{run_idx}_{datetime.now().strftime('%H%M%S')}"
                    log(f"\n   🔄 [Run {run_idx}/{NUM_REPEAT_TESTS}] ID: {run_id}")

                    # 1. Trigger (Dynamic Timeout & Strict ID Match)
                    # pod = helper.select_random_pod()
                    
                    # Pass the 'run_idx' so the helper can calculate the timeout
                    # helper.trigger_gossip_hybrid(pod, run_id, cycle_index=run_idx)

                    # 2. Propagation Wait
                    log(f"      ⏳ Propagating for {EXPERIMENT_DURATION}s...")
                    time.sleep(EXPERIMENT_DURATION)
                    
                    # 3. Buffer
                    log("      Waiting 5s buffer...")
                    time.sleep(5)

            except Exception as e:
                log(f"❌ FAILED: {filename} - {e}")
                if os.getcwd() != ROOT_DIR: os.chdir(ROOT_DIR)

            finally:
                log("🧹 Uninstalling Helm...")
                try:
                    helper.run_command("helm uninstall simcn")
                    time.sleep(20) 
                except: pass

    finally:
        log("🏁 Done.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\n🛑 Stopped by user.")