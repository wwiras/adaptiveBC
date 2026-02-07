import os
import glob
import time
import subprocess
import re
import random
import select
import logging
import sys
import json
import uuid
import string
import secrets
import csv
import argparse  # Added for dynamic arguments
from datetime import datetime, timezone, timedelta

# ==========================================
# 🔧 USER CONFIGURATION (Defaults)
# ==========================================
# These act as fallbacks if no arguments are provided
DEFAULT_PROJECT_ID = "stoked-cosine-415611"
DEFAULT_ZONE = "us-central1-c"
DEFAULT_K8SCLUSTER_NAME = "bcgossip-cluster"
DEFAULT_K8SNODE_COUNT = 3  
DEFAULT_P2P_NODES = 10 # Default pods

IMAGE_NAME = "wwiras/simcl2"
IMAGE_TAG = "v17"
TOPOLOGY_FOLDER = "topology"
HELM_CHART_FOLDER = "simcl2"
MTYPE = "e2-medium" 

EXPERIMENT_DURATION = 3    
BASE_TRIGGER_TIMEOUT = 10   
TIMEOUT_INCREMENT = 2       
NUM_REPEAT_TESTS = 3        

# ==========================================
# 🛠️ HELPERS & TIMEZONE
# ==========================================
MYT = timezone(timedelta(hours=8))

def get_short_id(length=5):
    characters = string.digits + string.ascii_letters
    return ''.join(secrets.choice(characters) for _ in range(length))

# ==========================================
# 📝 LOGGING SETUP
# ==========================================
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR, exist_ok=True)

unique_run_id = get_short_id(5)
timestamp_str = datetime.now(MYT).strftime("%Y%m%d_%H%M%S")

log_filename = f"orchestrator_{timestamp_str}_{unique_run_id}.log"
csv_filename = f"orchestrator_{timestamp_str}_{unique_run_id}.csv"
full_log_path = os.path.join(LOG_DIR, log_filename)
full_csv_path = os.path.join(LOG_DIR, csv_filename)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    handlers=[logging.FileHandler(full_log_path), logging.StreamHandler()]
)
def log(msg): logging.info(msg)

# ==========================================
# 🛠️ HELPER CLASS
# ==========================================
class ExperimentHelper:
    def run_command(self, command, shell=True, suppress_output=False, capture=True):
        try:
            result = subprocess.run(command, check=True, text=True, capture_output=capture, shell=shell)
            return result.stdout.strip() if capture else ""
        except subprocess.CalledProcessError as e:
            if not suppress_output:
                log(f"❌ Error executing: {e.cmd}\nStderr: {getattr(e, 'stderr', 'Check console')}")
            raise e

    def get_current_running_pod_count(self, namespace='default'):
        try:
            cmd = f"kubectl get pods -n {namespace} -l app=bcgossip --no-headers | grep Running | wc -l"
            count = self.run_command(cmd, suppress_output=True)
            return int(count) if count else 0
        except: return 0

    def wait_for_pods_to_be_ready(self, namespace='default', expected_pods=0, timeout=600):
        log(f"⏳ Waiting for {expected_pods} pods to reach Running state...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            running_pods = self.get_current_running_pod_count(namespace)
            if running_pods >= expected_pods:
                log(f"✅ Real-time Check: {running_pods}/{expected_pods} pods are READY.")
                return True
            time.sleep(5)
        return False

    def select_random_pod(self):
        cmd = "kubectl get pods -l app=bcgossip --no-headers | grep Running | awk '{print $1}'"
        stdout = self.run_command(cmd)
        pod_list = stdout.split()
        if not pod_list: raise Exception("No running pods found.")
        return random.choice(pod_list)

    def trigger_gossip_hybrid(self, pod_name, test_id, cycle_index):
        current_timeout = BASE_TRIGGER_TIMEOUT + ((cycle_index - 1) * TIMEOUT_INCREMENT)
        log(f"⚡ Triggering Gossip in {pod_name} (Msg: {test_id})")
        cmd = ['kubectl', 'exec', pod_name, '--', 'python3', 'start.py', '--message', test_id]
        start_time = time.time()
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        while True:
            if time.time() - start_time > current_timeout:
                process.kill()
                return False 
            reads = [process.stdout.fileno()]
            ready = select.select(reads, [], [], 1.0)[0]
            if ready:
                line = process.stdout.readline()
                if not line: break 
                if "Received acknowledgment" in line and test_id in line:
                    process.terminate() 
                    return True
            if process.poll() is not None: break
        return False

# ==========================================
# 🚀 MAIN ORCHESTRATOR
# ==========================================
def main():
    # --- ARGUMENT PARSING ---
    parser = argparse.ArgumentParser(description="Orchestrator for Blockchain Gossip Experiment")
    parser.add_argument("--k8snodes", type=int, default=DEFAULT_K8SNODE_COUNT, help="Number of GKE nodes")
    parser.add_argument("--p2pnodes", type=int, default=DEFAULT_P2P_NODES, help="Number of P2P pods")
    parser.add_argument("--zone", type=str, default=DEFAULT_ZONE, help="GCP Zone")
    parser.add_argument("--project_id", type=str, default=DEFAULT_PROJECT_ID, help="GCP Project ID")
    parser.add_argument("--cluster_name", type=str, default=DEFAULT_K8SCLUSTER_NAME, help="GKE Cluster Name")
    
    args = parser.parse_args()

    # Assign arguments to variables used in the script
    K8SNODE_COUNT = args.k8snodes
    ZONE = args.zone
    PROJECT_ID = args.project_id
    K8SCLUSTER_NAME = args.cluster_name
    # Note: p2pnodes is handled dynamically inside the topology loop, 
    # but we can use args.p2pnodes as a fallback if needed.

    helper = ExperimentHelper()
    ROOT_DIR = os.getcwd() 
    test_summary = []  
    
    # 1. TOPOLOGY SCANNING
    raw_files = glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json"))
    topology_list = []
    for filepath in raw_files:
        filename = os.path.basename(filepath)
        node_match = re.search(r"nodes(\d+)", filename)
        node_count = int(node_match.group(1)) if node_match else args.p2pnodes # Use dynamic arg if regex fails
        topology_list.append({"path": filepath, "filename": filename, "node_count": node_count})

    topology_list.sort(key=lambda x: x['node_count'])
    if not topology_list: return
    
    # 2. INFRASTRUCTURE SETUP
    log("\n" + "="*50)
    log("🏗️  INFRASTRUCTURE CONFIGURATION")
    log(f"   - Cluster Name: {K8SCLUSTER_NAME}")
    log(f"   - Nodes: {K8SNODE_COUNT}")
    log(f"   - Zone: {ZONE}")
    log(f"   - Project: {PROJECT_ID}")
    log("="*50 + "\n")

    try:
        subprocess.run([
            "gcloud", "container", "clusters", "create", K8SCLUSTER_NAME,
            "--zone", ZONE, "--num-nodes", str(K8SNODE_COUNT), 
            "--machine-type", MTYPE,"--quiet"
        ], check=True, capture_output=True, text=True)
        log("✅ Cluster created successfully.")
    except subprocess.CalledProcessError as e:
        if "already exists" in e.stderr.lower(): log("ℹ️ Cluster already exists.")
        else: sys.exit(1) 

    subprocess.run([
        "gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, 
        "--zone", ZONE, "--project", PROJECT_ID
    ], check=True)

    # 3. EXPERIMENT LOOP
    try:
        for i, topo in enumerate(topology_list):
            filename = topo['filename']
            p2p_nodes = topo['node_count']
            unique_id = get_short_id(5)
            base_test_id = f"{unique_id}-cubaan{p2p_nodes}"

            # Helm Deployment Logic...
            current_workload = helper.get_current_running_pod_count()
            if current_workload != p2p_nodes:
                try: helper.run_command("helm uninstall simcn", suppress_output=True)
                except: pass
                os.chdir(HELM_CHART_FOLDER)
                try:
                    helm_cmd = (f"helm install simcn ./chartsim --set testType=default,"
                                f"totalNodes={p2p_nodes},image.tag={IMAGE_TAG},image.name={IMAGE_NAME}")
                    helper.run_command(helm_cmd, capture=False)
                finally: os.chdir(ROOT_DIR)
                if not helper.wait_for_pods_to_be_ready(expected_pods=p2p_nodes): raise Exception("Scale failed")
            
            subprocess.run(f"python3 prepare.py --filename {filename}", shell=True, check=True)
            test_summary.append({"test_id": base_test_id, "topology": filename, "pods": p2p_nodes, "timestamp": datetime.now(MYT).strftime('%Y-%m-%d %H:%M:%S')})

            for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                message = f"{base_test_id}-{run_idx}"
                pod = helper.select_random_pod()
                helper.trigger_gossip_hybrid(pod, message, cycle_index=run_idx)
                time.sleep(EXPERIMENT_DURATION + 2)

    except Exception as e: log(f"❌ ERROR: {e}")
    finally:
        # Cleanup...
        subprocess.run(["helm", "uninstall", "simcn"], check=False)
        subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, "--zone", ZONE, "--project", PROJECT_ID, "--quiet"], check=False)
        # CSV Export Logic remains same...

if __name__ == "__main__":
    main()