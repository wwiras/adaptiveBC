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
# 📝 LOGGING & CSV SETUP
# ==========================================
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR, exist_ok=True)

unique_run_id = get_short_id(5)
timestamp_str = datetime.now(MYT).strftime("%Y%m%d_%H%M%S")

# Filenames linked by the same timestamp/ID
log_filename = f"orchestrator_{timestamp_str}_{unique_run_id}.log"
csv_filename = f"orchestrator_{timestamp_str}_{unique_run_id}.csv"

full_log_path = os.path.join(LOG_DIR, log_filename)
full_csv_path = os.path.join(LOG_DIR, csv_filename)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(full_log_path), 
        logging.StreamHandler()
    ]
)

logging.Formatter.converter = lambda *args: datetime.now(MYT).timetuple()

def log(msg):
    logging.info(msg)

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
    test_summary = []  
    
    # 1. TOPOLOGY SCANNING
    raw_files = glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json"))
    topology_list = []

    for filepath in raw_files:
        filename = os.path.basename(filepath)
        node_match = re.search(r"nodes(\d+)", filename)
        node_count = int(node_match.group(1)) if node_match else 0
        topology_list.append({
            "path": filepath,
            "filename": filename,
            "node_count": node_count
        })

    topology_list.sort(key=lambda x: x['node_count'])
    
    if not topology_list: 
        log("❌ No topology files found in the /topology folder.")
        return
    
    # 2. INFRASTRUCTURE SETUP
    log("\n" + "="*50)
    log("🏗️  INFRASTRUCTURE CONFIGURATION")
    log(f"   - Cluster Name: {K8SCLUSTER_NAME}")
    log(f"   - Nodes: {K8SNODE_COUNT}")
    log(f"   - Zone: {ZONE}")
    log(f"   - Project: {PROJECT_ID}")
    log(f"   - Time (MYT): {datetime.now(MYT).strftime('%d-%m-%Y %H:%M:%S')}")
    log("="*50 + "\n")

    try:
        subprocess.run([
            "gcloud", "container", "clusters", "create", K8SCLUSTER_NAME,
            "--zone", ZONE, "--num-nodes", str(K8SNODE_COUNT), 
            "--machine-type", "e2-medium", "--enable-ip-alias",
            "--max-pods-per-node", "40", "--enable-autoscaling",
            "--min-nodes", "1", "--max-nodes", "100", "--quiet"
        ], check=True, capture_output=True, text=True)
        log("✅ Cluster created successfully.")
    except subprocess.CalledProcessError as e:
        if "already exists" in e.stderr.lower():
            log("ℹ️ Cluster already exists. Fetching credentials...")
        else:
            log(f"❌ CRITICAL ERROR: {e.stderr}")
            sys.exit(1) 

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
            
            # This is the base ID used for extraction in BigQuery
            base_test_id = f"{unique_id}-cubaan{p2p_nodes}"

            log(f"\n[{i+1}/{len(topology_list)}] 🚀 TOPOLOGY: {filename}")
            log(f"   👉 Base Test ID: {base_test_id}")

            # --- A. CONDITIONAL HELM DEPLOYMENT ---
            current_workload = helper.get_current_running_pod_count()
            if current_workload != p2p_nodes:
                log(f"🔄 Scaling pods from {current_workload} to {p2p_nodes}...")
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
            subprocess.run(f"python3 prepare.py --filename {filename}", shell=True, check=True)

            # Record test metadata
            test_summary.append({
                "test_id": base_test_id,
                "topology": filename,
                "pods": p2p_nodes,
                "timestamp": datetime.now(MYT).strftime('%Y-%m-%d %H:%M:%S')
            })

            # --- C. REPEAT TEST LOOP ---
            for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                message = f"{base_test_id}-{run_idx}"
                log(f"   🔄 [Run {run_idx}/{NUM_REPEAT_TESTS}] Message: {message}")
                pod = helper.select_random_pod()
                helper.trigger_gossip_hybrid(pod, message, cycle_index=run_idx)

                log(f"      ⏳ Propagating for {EXPERIMENT_DURATION}s...")
                time.sleep(EXPERIMENT_DURATION + 2)

    except Exception as e:
        log(f"❌ CRITICAL ERROR DURING EXPERIMENT: {e}")
    
    finally:
        log("\n🧹 Starting Post-Experiment Cleanup...")
        try:
            subprocess.run(["helm", "uninstall", "simcn"], check=False)
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, 
                            "--zone", ZONE, "--project", PROJECT_ID, "--quiet"], check=False)
        except: pass

        # ==========================================
        # 📊 FINAL TEST SUMMARY & CSV EXPORT
        # ==========================================
        log("\n" + "="*80)
        log("📋 FINAL TEST EXECUTION SUMMARY")
        log(f"{'TEST_ID':<30} | {'TOPOLOGY':<40} | {'PODS':<5}")
        log("-" * 80)
        
        try:
            with open(full_csv_path, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["test_id", "topology", "pods", "timestamp"])
                writer.writeheader()
                for entry in test_summary:
                    # Print to logs
                    log(f"{entry['test_id']:<30} | {entry['topology']:<40} | {entry['pods']:<5}")
                    # Write to CSV
                    writer.writerow(entry)
            
            log("-" * 80)
            log(f"✅ CSV Exported to: {full_csv_path}")
        except Exception as e:
            log(f"❌ Error writing CSV: {e}")
        
        log("="*80)
        log("🏁 Done.")

if __name__ == "__main__":
    main()