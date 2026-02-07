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
import argparse
from datetime import datetime, timezone, timedelta

# ==========================================
# 🔧 PARAMETER PARSING
# ==========================================
def str2bool(v):
    if isinstance(v, bool): return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'): return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'): return False
    else: raise argparse.ArgumentTypeError('Boolean value expected.')

parser = argparse.ArgumentParser(description="Gossip Experiment Orchestrator")
parser.add_argument("--k8s_nodes", type=int, default=3, help="GKE worker nodes (default: 3)")
parser.add_argument("--p2p_nodes", type=int, default=10, help="Target P2P pods (default: 10)")
parser.add_argument("--autoscale", type=str2bool, default=False, help="Enable GKE autoscaling (default: false)")
parser.add_argument("--project_id", type=str, default="stoked-cosine-415611", help="GCP Project ID")
parser.add_argument("--zone", type=str, default="us-central1-c", help="GCP Zone")

args = parser.parse_args()
K8S_NODES = args.k8s_nodes
P2P_TARGET = args.p2p_nodes
AUTOSCALE_ENABLED = args.autoscale
PROJECT_ID = args.project_id
ZONE = args.zone

# ==========================================
# 🔧 USER CONFIGURATION
# ==========================================
K8SCLUSTER_NAME = "bcgossip-cluster"
IMAGE_NAME = "wwiras/simcl2"
IMAGE_TAG = "v17"
TOPOLOGY_FOLDER = "topology"
HELM_CHART_FOLDER = "simcl2" 

EXPERIMENT_DURATION = 3    
BASE_TRIGGER_TIMEOUT = 10   
TIMEOUT_INCREMENT = 2       
NUM_REPEAT_TESTS = 3        
MYT = timezone(timedelta(hours=8))

# ==========================================
# 📝 LOGGING SETUP
# ==========================================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

unique_run_id = ''.join(secrets.choice(string.digits + string.ascii_letters) for _ in range(5))
timestamp_str = datetime.now(MYT).strftime("%Y%m%d_%H%M%S")

log_filename = f"orchestrator_{timestamp_str}_{unique_run_id}.log"
csv_filename = f"orchestrator_{timestamp_str}_{unique_run_id}.csv"
full_log_path = os.path.join(LOG_DIR, log_filename)
full_csv_path = os.path.join(LOG_DIR, csv_filename)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(full_log_path), logging.StreamHandler()]
)

logging.Formatter.converter = lambda *args: datetime.now(MYT).timetuple()

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
    topology_list = [f for f in raw_files if f"nodes{P2P_TARGET}" in f]
    topology_list.sort()
    
    if not topology_list: 
        sys.exit(f"❌ No topology files found for {P2P_TARGET} nodes in /{TOPOLOGY_FOLDER}")
    
    # 2. INFRASTRUCTURE SETUP
    log("\n" + "="*50)
    log("🏗️  INFRASTRUCTURE CONFIGURATION")
    log(f"   - Cluster Name:   {K8SCLUSTER_NAME}")
    log(f"   - Node Count:     {K8S_NODES} (Autoscale: {AUTOSCALE_ENABLED})")
    log(f"   - Project ID:     {PROJECT_ID}")
    log(f"   - Zone:           {ZONE}")
    log(f"   - Execution Time: {datetime.now(MYT).strftime('%d-%m-%Y %H:%M:%S')}")
    log("="*50 + "\n")

    check_cmd = f"gcloud container clusters list --project {PROJECT_ID} --filter='name:{K8SCLUSTER_NAME}' --format='value(name)' --zone {ZONE}"
    existing = subprocess.run(check_cmd, shell=True, capture_output=True, text=True).stdout.strip()

    if not existing:
        log(f"🔨 Cluster not found. Initiating synchronous creation...")
        create_cmd = [
            "gcloud", "container", "clusters", "create", K8SCLUSTER_NAME,
            "--project", PROJECT_ID, "--zone", ZONE, "--num-nodes", str(K8S_NODES),
            "--machine-type", "e2-medium", "--enable-ip-alias",
            "--max-pods-per-node", "40", "--quiet"
        ]
        if AUTOSCALE_ENABLED:
            create_cmd.extend(["--enable-autoscaling", "--min-nodes", "1", "--max-nodes", "100"])
        
        try:
            subprocess.run(create_cmd, check=True)
            log("✅ Cluster created successfully.")
        except subprocess.CalledProcessError:
            sys.exit("❌ Infrastructure failure during creation.")
    else:
        log(f"ℹ️  Cluster '{K8SCLUSTER_NAME}' already exists. Reusing infrastructure.")

    subprocess.run(["gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, 
                    "--zone", ZONE, "--project", PROJECT_ID], check=True, capture_output=True)

    # 3. EXPERIMENT LOOP
    try:
        for i, filepath in enumerate(topology_list):
            filename = os.path.basename(filepath)
            unique_id = ''.join(secrets.choice(string.digits + string.ascii_letters) for _ in range(5))
            base_test_id = f"{unique_id}-cubaan{P2P_TARGET}"

            log(f"\n[{i+1}/{len(topology_list)}] 🚀 TOPOLOGY: {filename}")
            log(f"   👉 Base Test ID: {base_test_id}")

            # --- A. HELM DEPLOYMENT ---
            current_workload = helper.get_current_running_pod_count()
            log(f"🔄 Scaling pods for {P2P_TARGET} workload...")
            try:
                helper.run_command("helm uninstall simcn", suppress_output=True)
                time.sleep(5) 
            except: pass

            os.chdir(HELM_CHART_FOLDER)
            try:
                helm_cmd = (f"helm install simcn ./chartsim --set testType=default,"
                            f"totalNodes={P2P_TARGET},image.tag={IMAGE_TAG},image.name={IMAGE_NAME}")
                helper.run_command(helm_cmd, capture=False)
            finally:
                os.chdir(ROOT_DIR)

            if not helper.wait_for_pods_to_be_ready(expected_pods=P2P_TARGET):
                raise Exception("Pods scale-up failed.")
            
            # --- B. INJECT TOPOLOGY ---
            log(f"💉 Injecting Topology: {filename}")
            subprocess.run(f"python3 prepare.py --filename {filename}", shell=True, check=True)
            # Add small buffer to ensure SQLite tables are ready across all pods
            time.sleep(5)

            test_summary.append({
                "test_id": base_test_id,
                "topology": filename,
                "pods": P2P_TARGET,
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
            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            log("🚮 Requesting Cluster Deletion (Async background)...")
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, 
                            "--zone", ZONE, "--project", PROJECT_ID, "--quiet", "--async"], check=False)
        except: pass

        # Final CSV Export
        log("\n" + "="*80)
        log("📋 FINAL TEST EXECUTION SUMMARY")
        log(f"{'TEST_ID':<30} | {'TOPOLOGY':<40} | {'PODS':<5}")
        log("-" * 80)
        
        try:
            with open(full_csv_path, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["test_id", "topology", "pods", "timestamp"])
                writer.writeheader()
                for entry in test_summary:
                    log(f"{entry['test_id']:<30} | {entry['topology']:<40} | {entry['pods']:<5}")
                    writer.writerow(entry)
            log("-" * 80)
            log(f"✅ CSV Exported to: {full_csv_path}")
        except Exception as e:
            log(f"❌ Error writing CSV: {e}")
        
        log("="*80)
        log("🏁 Done.")

if __name__ == "__main__":
    main()