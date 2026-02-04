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
from datetime import datetime, timezone, timedelta

# ==========================================
# 🔧 USER CONFIGURATION
# ==========================================
PROJECT_ID = "stoked-cosine-415611"
ZONE = "us-central1-c"
K8SCLUSTER_NAME = "bcgossip-cluster"
K8SNODE_COUNT = 5  

IMAGE_NAME = "wwiras/simcl2"
IMAGE_TAG = "v17"
TOPOLOGY_FOLDER = "topology"
HELM_CHART_FOLDER = "simcl2" 

EXPERIMENT_DURATION = 10    
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
# 📝 LOGGING SETUP (MYT & /logs Directory)
# ==========================================
# 1. Define and create the directory
LOG_DIR = "logs" # Or "/logs" if you mean the root directory
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR, exist_ok=True)

# 2. Generate the unique filename
unique_run_id = get_short_id(5)
timestamp_str = datetime.now(MYT).strftime("%Y%m%d_%H%M%S")
log_filename = f"orchestrator_{timestamp_str}_{unique_run_id}.log"

# 3. Join path: result will be "logs/orchestrator_K9b2X_20260203_001500.log"
full_log_path = os.path.join(LOG_DIR, log_filename)

# Update your FileHandler to use delay=False or flush manually
file_handler = logging.FileHandler(full_log_path)
stream_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[file_handler, stream_handler]
)

# Ensure internal log timestamps use MYT
logging.Formatter.converter = lambda *args: datetime.now(MYT).timetuple()

# Add this small helper to force a disk write after every log call
def log(msg):
    logging.info(msg)
    for handler in logging.getLogger().handlers:
        handler.flush()


# ==========================================
# 🛠️ HELPER CLASS
# ==========================================
class ExperimentHelper:
    def run_command(self, command, shell=True, suppress_output=False, capture=True):
        try:
            result = subprocess.run(
                command, 
                check=True, 
                text=True, 
                capture_output=capture, 
                shell=shell
            )
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
        except:
            return 0

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
        # test_id is already the formatted message from the main loop
        log(f"⚡ Triggering Gossip in {pod_name} (Msg: {test_id})")
        
        cmd = ['kubectl', 'exec', pod_name, '--', 'python3', 'start.py', '--message', test_id]
        start_time = time.time()
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

        while True:
            if time.time() - start_time > current_timeout:
                log(f"⏱️ Trigger Timeout ({current_timeout}s) reached.")
                process.kill()
                return False 

            reads = [process.stdout.fileno()]
            ready = select.select(reads, [], [], 1.0)[0]

            if ready:
                line = process.stdout.readline()
                if not line: break 
                if "Received acknowledgment" in line and test_id in line:
                    log(f"✅ VALID ACK RECEIVED for {test_id}!")
                    process.terminate() 
                    return True
            
            if process.poll() is not None: break
        return False

# ==========================================
# 🚀 MAIN ORCHESTRATOR
# ==========================================
def main():
    helper = ExperimentHelper()
    ROOT_DIR = os.getcwd() 
    
    # 1. GROUP & SORT TOPOLOGIES
    raw_files = glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json"))
    topology_list = []

    for filepath in raw_files:
        filename = os.path.basename(filepath)
        node_match = re.search(r"nodes(\d+)", filename)
        node_count = int(node_match.group(1)) if node_match else 0
        topology_list.append({
            "path": filepath,
            "filename": filename,
            "node_count": node_count,
            "unique_id": ''
        })

    topology_list.sort(key=lambda x: x['node_count'])
    
    if not topology_list: 
        log("❌ No topology files found.")
        return
    
    # 2. INFRASTRUCTURE SETUP
    log("\n" + "="*50)
    log("🏗️  INFRASTRUCTURE CONFIGURATION")
    log(f"   - Cluster Name: {K8SCLUSTER_NAME}")
    log(f"   - Zone:         {ZONE}")
    log(f"   - Nodes: {K8SNODE_COUNT}")
    log(f"   - Machine Type: e2-medium")
    log(f"   - Project ID:   {PROJECT_ID}")
    log(f"   - Image:        {IMAGE_NAME}:{IMAGE_TAG}")
    log(f"   - Date and Time: {datetime.now(MYT).strftime("%d-%m-%Y at %H:%M:%S")}")
    log("="*50 + "\n")
    log(f"🔨 Ensuring Cluster {K8SCLUSTER_NAME} (Progress shown below)...")
    
    # Check if cluster exists (this is fast and captured)
    check_cmd = f"gcloud container clusters list --filter='name:{K8SCLUSTER_NAME}' --format='value(name)' --zone {ZONE}"
    existing = subprocess.run(check_cmd, shell=True, capture_output=True, text=True).stdout.strip()

    if not existing:
        log(f"🚀 Cluster not found. Building fresh (Progress shown below)...")
        try:
            subprocess.run([
                "gcloud", "container", "clusters", "create", K8SCLUSTER_NAME,
                "--zone", ZONE, "--num-nodes", str(K8SNODE_COUNT), 
                "--machine-type", "e2-medium", "--enable-ip-alias",
                "--max-pods-per-node", "40", "--enable-autoscaling",
                "--min-nodes", "1", "--max-nodes", "100", "--quiet"
            ], check=True) # capture_output is False by default here
            log("✅ Cluster created successfully.")
        except subprocess.CalledProcessError:
            log("❌ CRITICAL ERROR: gcloud failed to create the cluster.")
            sys.exit(1)
    else:
        log(f"ℹ️  Cluster '{K8SCLUSTER_NAME}' already exists. Skipping build.")
   
       # Now this command will only run if the cluster actually exists
    subprocess.run([
        "gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, 
        "--zone", ZONE, 
        "--project", PROJECT_ID
    ], check=True)

    # 3. EXPERIMENT LOOP
    try:
        for i, topo in enumerate(topology_list):
            filename = topo['filename']
            p2p_nodes = topo['node_count']
            # unique_id = str(uuid.uuid4())[:5]
            unique_id = get_short_id(5)

            log(f"\n[{i+1}/{len(topology_list)}] 🚀 STARTING TOPOLOGY: {filename} with ID:{unique_id}")

            # --- A. CONDITIONAL HELM DEPLOYMENT ---
            current_workload = helper.get_current_running_pod_count()
            if current_workload != p2p_nodes:
                log(f"🔄 Scaling Workload to {p2p_nodes} pods...")
                try:
                    helper.run_command("helm uninstall simcn", suppress_output=True)
                    time.sleep(5) 
                except: pass

                os.chdir(HELM_CHART_FOLDER)
                try:
                    helm_cmd = (f"helm install simcn ./chartsim --set testType=default,"
                                f"totalNodes={p2p_nodes},image.tag={IMAGE_TAG},image.name={IMAGE_NAME}")
                    helper.run_command(helm_cmd, capture=False)
                finally:
                    os.chdir(ROOT_DIR)

                if not helper.wait_for_pods_to_be_ready(expected_pods=p2p_nodes):
                    raise Exception("Pods scale-up failed.")
            
            # --- B. INJECT TOPOLOGY ---
            log(f"💉 Injecting Topology: {filename}")
            start_inj = time.time()
            
            # Using capture_output=False (default) lets it print to console
            # check=True ensures we catch failures immediately
            res = subprocess.run(
                f"python3 prepare.py --filename {filename}", 
                shell=True, 
                check=True,
                capture_output=False,
                text=True
            )
            
            duration_inj = time.time() - start_inj
            log(f"✅ Injection completed in {duration_inj:.2f} seconds.")

            # --- C. REPEAT TEST LOOP ---
            for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                # Format: unique_id-cubaan[replicas]-[iteration]
                # Example: z66ef_BA4-cubaan10-1
                test_id = f"{unique_id}-cubaan{p2p_nodes}-{run_idx}"
                
                log(f"\n   🔄 [Run {run_idx}/{NUM_REPEAT_TESTS}] Message: {test_id}")

                # Select pod and Trigger with the new test_id message
                pod = helper.select_random_pod()
                helper.trigger_gossip_hybrid(pod, test_id, cycle_index=run_idx)

                log(f"      ⏳ Propagating for {EXPERIMENT_DURATION}s...")
                time.sleep(EXPERIMENT_DURATION)
                time.sleep(2) # Short buffer

    except Exception as e:
        log(f"❌ CRITICAL ERROR: {e}")
    
    finally:
        log("\n🧹 Starting Post-Experiment Cleanup...")
        try:
            subprocess.run(["helm", "uninstall", "simcn"], check=False)
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, "--zone", ZONE, "--project", PROJECT_ID, "--quiet"], check=True)
        except: pass
        log("🏁 Done.")

if __name__ == "__main__":
    main()