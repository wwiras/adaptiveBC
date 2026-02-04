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
# 📝 LOGGING SETUP
# ==========================================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

unique_run_id = get_short_id(5)
timestamp_str = datetime.now(MYT).strftime("%Y%m%d_%H%M%S")
log_filename = f"orchestrator_{timestamp_str}_{unique_run_id}.log"
full_log_path = os.path.join(LOG_DIR, log_filename)

file_handler = logging.FileHandler(full_log_path)
stream_handler = logging.StreamHandler()
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S", handlers=[file_handler, stream_handler])
logging.Formatter.converter = lambda *args: datetime.now(MYT).timetuple()

def log(msg):
    logging.info(msg)
    for handler in logging.getLogger().handlers: handler.flush()

# ==========================================
# 🛠️ EXPERIMENT HELPER
# ==========================================
class ExperimentHelper:
    def run_command(self, command, shell=True, capture=True):
        result = subprocess.run(command, check=True, text=True, capture_output=capture, shell=shell)
        return result.stdout.strip() if capture else ""

    def get_running_pods(self):
        cmd = "kubectl get pods --no-headers | grep Running | wc -l"
        return int(self.run_command(cmd))

    def wait_for_ready(self, expected, timeout=600):
        start = time.time()
        while time.time() - start < timeout:
            if self.get_running_pods() >= expected: return True
            time.sleep(5)
        return False

    def trigger_gossip_hybrid(self, pod, test_id, cycle):
        timeout = BASE_TRIGGER_TIMEOUT + ((cycle - 1) * TIMEOUT_INCREMENT)
        log(f"⚡ Triggering {pod} (Timeout: {timeout}s)...")
        cmd = ['kubectl', 'exec', pod, '--', 'python3', 'start.py', '--message', test_id]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        start = time.time()
        while time.time() - start < timeout:
            r, _, _ = select.select([proc.stdout], [], [], 1.0)
            if r:
                line = proc.stdout.readline()
                if "Received acknowledgment" in line and test_id in line:
                    log(f"✅ ACK RECEIVED for {test_id}")
                    proc.terminate()
                    return True
            if proc.poll() is not None: break
        proc.kill()
        log(f"⏱️ Trigger Timeout/Exit for {test_id}")
        return False

# ==========================================
# 🚀 MAIN ORCHESTRATOR
# ==========================================
def main():
    helper = ExperimentHelper()
    ROOT_DIR = os.getcwd()
    summary_data = [] # To store IDs for BigQuery reference

    # 1. SCAN TOPOLOGIES
    files = sorted(glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json")))
    
    # 2. INFRASTRUCTURE SETUP
    log(f"🔨 Ensuring Cluster {K8SCLUSTER_NAME}...")
    
    # A. Check if cluster exists
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
                "--min-nodes", "1", "--max-nodes", "100", 
                "--autoscaling-profile", "optimize-utilization",
                "--quiet"
            ], check=True)
            log("✅ Cluster created successfully.")
        except subprocess.CalledProcessError:
            log("❌ CRITICAL ERROR: gcloud failed to create the cluster.")
            sys.exit(1)
    else:
        log(f"ℹ️ Cluster '{K8SCLUSTER_NAME}' already exists. Skipping build.")

    # B. Fetch credentials
    subprocess.run(["gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, "--zone", ZONE], check=True)
    
    # 2. CLUSTER SETUP
    # log(f"🔨 Ensuring Cluster {K8SCLUSTER_NAME}...")
    # (Omitted cluster creation logic for brevity - use your existing gcloud check)
    # subprocess.run(["gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, "--zone", ZONE], check=True)


    # STEP 4: TOPOLOGY INJECTION
    try:
        for topo_file in files:
            fname = os.path.basename(topo_file)
            
            # Extract Meta from Filename (e.g., nodes10_k2_d3.json)
            nodes = int(re.search(r"nodes(\d+)", fname).group(1)) if "nodes" in fname else 0
            clusters = int(re.search(r"_k(\d+)", fname).group(1)) if "_k" in fname else 1
            degree = int(re.search(r"_d(\d+)", fname).group(1)) if "_d" in fname else 0
            
            unique_id = get_short_id(5)
            log(f"\n🚀 TOPOLOGY: {fname} | ID: {unique_id}")

            # A. HELM DEPLOY
            os.chdir(HELM_CHART_FOLDER)
            try:
                helper.run_command("helm uninstall simcn", capture=False)
                time.sleep(5)
            except: pass
            
            helm_cmd = f"helm install simcn ./chartsim --set totalNodes={nodes},image.tag={IMAGE_TAG}"
            helper.run_command(helm_cmd, capture=False)
            os.chdir(ROOT_DIR)
            helper.wait_for_ready(nodes)

            # B. INJECT
            subprocess.run(f"python3 prepare.py --filename {topo_file}", shell=True, check=True)

            # C. RUN TESTS
            for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                test_id = f"{unique_id}-n{nodes}-k{clusters}-r{run_idx}"
                log(f"🔄 Run {run_idx}/{NUM_REPEAT_TESTS} | ID: {test_id}")
                
                pod = helper.run_command("kubectl get pods -l app=bcgossip -o name | head -n 1").split('/')[-1]
                helper.trigger_gossip_hybrid(pod, test_id, run_idx)
                
                time.sleep(EXPERIMENT_DURATION)
                
                # Save to summary for BigQuery querying later
                summary_data.append([test_id, fname, nodes, clusters, degree])

    except Exception as e:
        log(f"❌ ERROR: {e}")
    
    finally:
        # STEP 5: SHUTDOWN FIRST (Save Budget!)
        log("\n🛑 STEP 5: SHUTTING DOWN CLUSTER...")
        try:
            subprocess.run(["helm", "uninstall", "simcn"], check=False)
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, "--zone", ZONE, "--quiet"], check=True)
            log("✅ Cluster deleted.")
        except: pass

        # STEP 6: DATA REFERENCE SUMMARY
        log("\n" + "="*80)
        log("📊 STEP 6: EXPERIMENT SUMMARY (Use for BigQuery Queries)")
        log(f"{'TEST_ID':<30} | {'TOPOLOGY':<25} | {'NODES':<5} | {'K':<3} | {'D':<3}")
        log("-" * 80)
        for row in summary_data:
            log(f"{row[0]:<30} | {row[1]:<25} | {row[2]:<5} | {row[3]:<3} | {row[4]:<3}")
        log("="*80)
        log("🏁 All tasks complete.")

if __name__ == "__main__":
    main()