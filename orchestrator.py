import os
import glob
import time
import subprocess
import re
import random
import logging
import sys
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
IMAGE_TAG = "v18"
TOPOLOGY_FOLDER = "topology"
HELM_CHART_FOLDER = "simcl2" 

EXPERIMENT_DURATION = 10    
BASE_TRIGGER_TIMEOUT = 12   
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

def log(msg): 
    logging.info(msg)
    for handler in logging.getLogger().handlers:
        handler.flush()

# ==========================================
# 🛠️ GOSSIP HELPERS
# ==========================================
def select_random_pod():
    cmd = "kubectl get pods -l app=bcgossip --no-headers | grep Running | awk '{print $1}'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    pod_list = result.stdout.strip().split()
    return random.choice(pod_list) if pod_list else None

def trigger_gossip_hybrid(pod_name, test_id):
    log(f"      ⚡ Triggering {pod_name} (Msg: {test_id})")
    cmd = ['kubectl', 'exec', pod_name, '--', 'python3', 'start.py', '--message', test_id]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=BASE_TRIGGER_TIMEOUT)
        if "Received acknowledgment" in proc.stdout:
            log(f"      ✅ ACK RECEIVED")
            return True
        log(f"      ⚠️ No ACK. Pod said: {proc.stdout.strip()[:50]}")
    except subprocess.TimeoutExpired:
        log("      ⏱️ Trigger Timeout")
    return False

def wait_for_pods_ready(expected, timeout=300):
    log(f"⏳ Waiting for {expected} pods to reach 'Running' state...")
    start = time.time()
    while time.time() - start < timeout:
        cmd = "kubectl get pods -l app=bcgossip --no-headers | grep Running | wc -l"
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        count = int(res.stdout.strip() or 0)
        if count >= expected:
            log(f"✅ All {expected} pods are READY.")
            return True
        time.sleep(10)
    return False

# ==========================================
# 🚀 MAIN ORCHESTRATOR
# ==========================================
def main():
    raw_files = glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json"))
    topology_list = [f for f in raw_files if f"nodes{P2P_TARGET}" in f]

    if not topology_list:
        sys.exit(f"❌ No topology files found for {P2P_TARGET} nodes in /{TOPOLOGY_FOLDER}")

    # 2. INFRASTRUCTURE SETUP
    log("\n" + "="*50)
    log("🏗️  INFRASTRUCTURE CONFIGURATION")
    log(f"   - Cluster Name:   {K8SCLUSTER_NAME}")
    log(f"   - Node Count:     {K8S_NODES} (Autoscale: {AUTOSCALE_ENABLED})")
    log(f"   - Target P2P:     {P2P_TARGET} pods")
    log(f"   - Project ID:     {PROJECT_ID}")
    log(f"   - Zone:           {ZONE}")
    log(f"   - Execution Time: {datetime.now(MYT).strftime('%d-%m-%Y %H:%M:%S')}")
    log("="*50 + "\n")

    check_cmd = f"gcloud container clusters list --filter='name:{K8SCLUSTER_NAME}' --format='value(name)' --zone {ZONE}"
    existing = subprocess.run(check_cmd, shell=True, capture_output=True, text=True).stdout.strip()

    if not existing:
        log(f"🔨 Cluster not found. Initiating creation (this takes 5-8 mins)...")
        create_cmd = [
            "gcloud", "container", "clusters", "create", K8SCLUSTER_NAME,
            "--zone", ZONE, "--num-nodes", str(K8S_NODES),
            "--machine-type", "e2-medium", "--enable-ip-alias", "--quiet"
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

    test_summary = []
    try:
        for filepath in topology_list:
            filename = os.path.basename(filepath)
            unique_id = ''.join(secrets.choice(string.digits + string.ascii_letters) for _ in range(5))
            base_test_id = f"{unique_id}-cubaan{P2P_TARGET}"

            log(f"\n🚀 STARTING TOPOLOGY: {filename} (ID: {base_test_id})")
            
            log(f"🔄 Helm: Reinstalling simcn for {P2P_TARGET} pods...")
            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            time.sleep(5)
            
            os.chdir(HELM_CHART_FOLDER)
            subprocess.run(f"helm install simcn ./chartsim --set totalNodes={P2P_TARGET},image.tag={IMAGE_TAG}", 
                           shell=True, check=True, capture_output=True)
            os.chdir("..")

            if not wait_for_pods_ready(P2P_TARGET):
                log(f"❌ Pods failed to initialize for {filename}. Skipping.")
                continue

            log(f"💉 Injecting Topology: {filename}")
            subprocess.run(f"python3 prepare.py --filename {filename}", shell=True, check=True)

            for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                msg = f"{base_test_id}-{run_idx}"
                log(f"   🔄 Run {run_idx}/{NUM_REPEAT_TESTS}: {msg}")
                
                target_pod = select_random_pod()
                if target_pod:
                    if trigger_gossip_hybrid(target_pod, msg):
                        log(f"      ⏳ Propagating for {EXPERIMENT_DURATION}s...")
                        time.sleep(EXPERIMENT_DURATION)
                        
                        test_summary.append({
                            "test_id": msg, 
                            "topology": filename, 
                            "pods": P2P_TARGET, 
                            "timestamp": datetime.now(MYT).strftime('%Y-%m-%d %H:%M:%S')
                        })
                else:
                    log("      ❌ Error: No running pods found to trigger.")
                
                time.sleep(2)

    except Exception as e:
        log(f"❌ ERROR DURING LOOP: {e}")
    finally:
        log("\n🧹 CLEANUP: Deleting Helm release and Cluster...")
        try:
            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, 
                            "--zone", ZONE, "--project", PROJECT_ID, "--quiet", "--async"], check=False)
            log("🚮 Cluster deletion requested (GCP is handling it in background).")
        except: pass

        with open(full_csv_path, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["test_id", "topology", "pods", "timestamp"])
            writer.writeheader()
            writer.writerows(test_summary)
        
        log(f"📊 Summary exported to: {full_csv_path}")
        log("🏁 Done.")

if __name__ == "__main__":
    main()