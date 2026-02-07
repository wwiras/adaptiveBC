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

args = parser.parse_args()
K8S_NODES = args.k8s_nodes
P2P_TARGET = args.p2p_nodes
AUTOSCALE_ENABLED = args.autoscale

# ==========================================
# 🔧 USER CONFIGURATION
# ==========================================
PROJECT_ID = "stoked-cosine-415611"
ZONE = "us-central1-c"
K8SCLUSTER_NAME = "bcgossip-cluster"
IMAGE_NAME = "wwiras/simcl2"
IMAGE_TAG = "v17"
TOPOLOGY_FOLDER = "topology"
HELM_CHART_FOLDER = "simcl2" 

EXPERIMENT_DURATION = 3    
BASE_TRIGGER_TIMEOUT = 10   
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

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S",
                    handlers=[logging.FileHandler(full_log_path), logging.StreamHandler()])
logging.Formatter.converter = lambda *args: datetime.now(MYT).timetuple()

def log(msg): logging.info(msg)

# ==========================================
# 🛠️ CLUSTER ASYNC LOGIC
# ==========================================
def create_cluster_async(cluster_name, zone, node_count, autoscale, timeout_seconds=900):
    log(f"🚀 Initiating cluster creation (Async Mode)...")
    
    gke_cmd = [
        "gcloud", "container", "clusters", "create", cluster_name,
        "--zone", zone, "--num-nodes", str(node_count),
        "--machine-type", "e2-medium", "--enable-ip-alias", "--quiet", "--async"
    ]
    if autoscale:
        gke_cmd.extend(["--enable-autoscaling", "--min-nodes", "1", "--max-nodes", "100"])

    try:
        # We capture stderr to check if the cluster already exists
        proc = subprocess.run(gke_cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            if "already exists" in proc.stderr.lower():
                log("ℹ️ Cluster already exists. Skipping creation.")
                return True
            else:
                log(f"❌ Initial request failed: {proc.stderr}")
                return False
    except Exception as e:
        log(f"❌ Subprocess error: {e}")
        return False

    log("📡 Request accepted by GCP. Polling for 'RUNNING' status...")
    start_time = time.time()
    
    while (time.time() - start_time) < timeout_seconds:
        status_check = subprocess.run([
            "gcloud", "container", "clusters", "describe", cluster_name,
            "--zone", zone, "--format=value(status)"
        ], capture_output=True, text=True)
        
        status = status_check.stdout.strip()
        elapsed = int(time.time() - start_time)
        
        if status == "RUNNING":
            log(f"✅ Cluster is READY after {elapsed}s.")
            return True
        elif status in ["PROVISIONING", "RECONCILING"]:
            log(f"⏳ Status: {status} ({elapsed}s elapsed)...")
        else:
            log(f"❓ Current Status: {status}")
            
        time.sleep(30)
    
    log(f"❌ TIMEOUT: Cluster did not reach RUNNING in {timeout_seconds}s.")
    return False

# ==========================================
# 🚀 MAIN ORCHESTRATOR
# ==========================================
def main():
    # --- PRE-FLIGHT CHECKS ---
    if P2P_TARGET > (K8S_NODES * 35) and not AUTOSCALE_ENABLED:
        log(f"⚠️ DANGER: {P2P_TARGET} pods is likely too many for {K8S_NODES} nodes.")
        log("Consider increasing --k8s_nodes or enabling --autoscale true.")
        sys.exit(1)

    # 1. TOPOLOGY SCANNING
    raw_files = glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json"))
    topology_list = [f for f in raw_files if f"nodes{P2P_TARGET}" in f]

    if not topology_list:
        log(f"❌ No topology files found for {P2P_TARGET} nodes in /{TOPOLOGY_FOLDER}")
        return

    # 2. INFRASTRUCTURE SETUP
    log("\n" + "="*50)
    log("🏗️  INFRASTRUCTURE CONFIGURATION")
    log(f"   - K8s Nodes: {K8S_NODES}")
    log(f"   - P2P Target: {P2P_TARGET}")
    log(f"   - Autoscale:  {AUTOSCALE_ENABLED}")
    log(f"   - Project:    {PROJECT_ID}")
    log("="*50 + "\n")

    if not create_cluster_async(K8SCLUSTER_NAME, ZONE, K8S_NODES, AUTOSCALE_ENABLED):
        sys.exit("Infrastructure failure. Exiting.")

    # Get credentials quietly
    subprocess.run(["gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, 
                    "--zone", ZONE, "--project", PROJECT_ID], check=True, capture_output=True)

    # 3. EXPERIMENT LOOP
    test_summary = []
    try:
        for filepath in topology_list:
            filename = os.path.basename(filepath)
            unique_id = ''.join(secrets.choice(string.digits + string.ascii_letters) for _ in range(5))
            base_test_id = f"{unique_id}-cubaan{P2P_TARGET}"

            log(f"\n🚀 STARTING TOPOLOGY: {filename} (ID: {base_test_id})")
            
            # Helm Deployment
            log(f"🔄 Helm: Reinstalling simcn for {P2P_TARGET} pods...")
            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            time.sleep(5)
            
            os.chdir(HELM_CHART_FOLDER)
            subprocess.run(f"helm install simcn ./chartsim --set totalNodes={P2P_TARGET},image.tag={IMAGE_TAG}", 
                           shell=True, check=True, capture_output=True)
            os.chdir("..")

            # Pod Ready Check (simplified polling)
            log(f"⏳ Waiting for Pods to initialize...")
            time.sleep(40) 

            # Inject Topology
            subprocess.run(f"python3 prepare.py --filename {filename}", shell=True, check=True)

            test_summary.append({"test_id": base_test_id, "topology": filename, "pods": P2P_TARGET, 
                                 "timestamp": datetime.now(MYT).strftime('%Y-%m-%d %H:%M:%S')})

            # Repeat Tests (Repeat logic based on previous trigger scripts)
            for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                msg = f"{base_test_id}-{run_idx}"
                log(f"   🔄 Run {run_idx}: {msg}")
                # [Triggering logic would go here]
                time.sleep(EXPERIMENT_DURATION + 2)

    except Exception as e:
        log(f"❌ ERROR DURING LOOP: {e}")
    finally:
        log("\n🧹 CLEANUP: Deleting Helm release and Cluster...")
        try:
            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            # Delete cluster asynchronously to save local time, or remove --async to wait
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, 
                            "--zone", ZONE, "--project", PROJECT_ID, "--quiet", "--async"], check=False)
            log("🚮 Cluster deletion requested (Async).")
        except: pass

        # Final CSV Summary
        with open(full_csv_path, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["test_id", "topology", "pods", "timestamp"])
            writer.writeheader()
            writer.writerows(test_summary)
        log(f"📊 Summary exported to: {full_csv_path}")
        log("🏁 Done.")

if __name__ == "__main__":
    main()