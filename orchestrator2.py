import os
import glob
import time
import subprocess
import json
import string
import secrets
import csv
import argparse
import logging
from datetime import datetime, timezone, timedelta

# ==========================================
# 🔧 PARAMETERS & CONFIG
# ==========================================
parser = argparse.ArgumentParser()
parser.add_argument("--k8snodes", type=int, default=3)
parser.add_argument("--zone", type=str, default="us-central1-c")
parser.add_argument("--project_id", type=str, default="stoked-cosine-415611")
parser.add_argument("--cluster_name", type=str, default="bcgossip-cluster")
args = parser.parse_args()

PROJECT_ID = args.project_id
ZONE = args.zone
K8SCLUSTER_NAME = args.cluster_name
K8SNODE_COUNT = args.k8snodes

IMAGE_TAG = "v22"
TOPOLOGY_FOLDER = "topology"
HELM_CHART_FOLDER = "simcl2"
REMOTE_PROJECT_DIR = "~/adaptiveBCproj/adaptiveBC" # Path in Cloud Shell
EXPERIMENT_DURATION = 10
NUM_REPEAT_TESTS = 3
MYT = timezone(timedelta(hours=8))

# ==========================================
# 📝 LOGGING SETUP
# ==========================================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
timestamp_str = datetime.now(MYT).strftime("%Y%m%d_%H%M%S")
unique_run_id = ''.join(secrets.choice(string.digits + string.ascii_letters) for _ in range(5))

full_log_path = os.path.join(LOG_DIR, f"orchestrator_{timestamp_str}_{unique_run_id}.log")
full_csv_path = os.path.join(LOG_DIR, f"orchestrator_{timestamp_str}_{unique_run_id}.csv")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S",
                    handlers=[logging.FileHandler(full_log_path), logging.StreamHandler()])

def log(msg):
    logging.info(msg)
    for handler in logging.getLogger().handlers: handler.flush()

# ==========================================
# 🚀 MAIN ORCHESTRATOR
# ==========================================
def main():
    ROOT_DIR = os.getcwd()
    test_summary = []
    last_p2p_count = 0 

    log("\n" + "="*60)
    log(f"🏗️  GKE HYBRID ORCHESTRATOR | Project: {PROJECT_ID}")
    log(f"   Mode: Cloud Shell Offloading (500-node stable)")
    log("="*60 + "\n")

    # 1. Infrastructure Sync & Setup
    try:
        log("🔨 Checking GKE Cluster...")
        subprocess.run(["gcloud", "container", "clusters", "create", K8SCLUSTER_NAME, "--project", PROJECT_ID,
                        "--zone", ZONE, "--num-nodes", str(K8SNODE_COUNT), "--machine-type", "e2-medium", 
                        "--enable-autoscaling", "--min-nodes", "1", "--max-nodes", "20", "--quiet"], 
                       check=True, capture_output=True)
        log("✅ Cluster created.")
    except:
        log("ℹ️ Reusing existing cluster.")

    subprocess.run(["gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, "--zone", ZONE, "--project", PROJECT_ID], check=True, capture_output=True)

    # 2. Experiment Loop
    topology_files = sorted(glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json")))
    
    try:
        for filepath in topology_files:
            filename = os.path.basename(filepath)
            with open(filepath, 'r') as f:
                topo_data = json.load(f)
            expected_nodes = len(topo_data.get('nodes', []))
            
            if expected_nodes == 0: continue

            run_uid = ''.join(secrets.choice(string.digits + string.ascii_letters) for _ in range(5))
            base_id = f"{run_uid}-nodes{expected_nodes}"
            log(f"\n" + "-"*40)
            log(f"📁 FILE: {filename} ({expected_nodes} nodes)")
            log("-"*40)

            # --- A. CONDITIONAL HELM SCALING ---
            if expected_nodes != last_p2p_count:
                log(f"🔄 Scaling: {last_p2p_count} -> {expected_nodes}. Reinstalling Helm...")
                subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
                
                # Termination Barrier
                log("⏳ Waiting for zero-pod state...")
                while True:
                    check = subprocess.run("kubectl get pods -l app=bcgossip --no-headers", shell=True, capture_output=True, text=True)
                    if not check.stdout.strip(): break
                    time.sleep(5)

                os.chdir(HELM_CHART_FOLDER)
                subprocess.run(f"helm install simcn ./chartsim --set totalNodes={expected_nodes},image.tag={IMAGE_TAG}", shell=True, check=True, capture_output=True)
                os.chdir(ROOT_DIR)

                # Ready Barrier
                log(f"⏳ Waiting for {expected_nodes} pods to reach 'Running'...")
                wait_start = time.time()
                ready_success = False
                while time.time() - wait_start < (120 + expected_nodes*2):
                    res = subprocess.run("kubectl get pods -l app=bcgossip --no-headers | grep Running | wc -l", shell=True, capture_output=True, text=True)
                    count = int(res.stdout.strip() or 0)
                    if count == expected_nodes:
                        log(f"✅ Cluster scaled and ready.")
                        time.sleep(15) # Stabilization cooldown
                        ready_success = True
                        break
                    time.sleep(10)
                
                if not ready_success:
                    log("🛑 Scale-up timed out. Skipping.")
                    last_p2p_count = 0
                    continue
                last_p2p_count = expected_nodes
            else:
                log(f"⚡ Warm Start: Keeping existing {expected_nodes} pods.")

            # --- B. CLOUD SHELL INJECTION (The Exploit) ---
            injection_verified = False
            max_attempts = 2
            
            for attempt in range(1, max_attempts + 1):
                log(f"💉 [Attempt {attempt}/{max_attempts}] Offloading to Cloud Shell...")
                
                # Chain commands: get credentials -> cd to project -> run prepare
                remote_cmd = (
                    f"gcloud cloud-shell ssh --authorize-session --command='"
                    f"gcloud container clusters get-credentials {K8SCLUSTER_NAME} --zone {ZONE} --project {PROJECT_ID} && "
                    f"cd {REMOTE_PROJECT_DIR} && "
                    f"python3 prepare_new.py --filename {filename}'"
                )
                
                process = subprocess.Popen(remote_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                
                for line in process.stdout:
                    clean_line = line.strip()
                    if clean_line: print(f"   [CloudShell] {clean_line}")
                    
                    # Verify success based on the new summary format
                    if "Overall Summary" in clean_line and f"DB Update Success: {expected_nodes}" in clean_line:
                        injection_verified = True
                
                process.wait()
                if injection_verified: break
                log("⚠️ Injection incomplete. Re-trying...")
                time.sleep(5)

            if not injection_verified:
                log(f"🛑 ABORT: Injection failed for {filename}. Moving to next.")
                last_p2p_count = 0
                continue

            # --- C. TRIGGER GOSSIP ---
            # Fetch pod list to pick a target
            cmd = "kubectl get pods -l app=bcgossip -o jsonpath='{.items[0].metadata.name}'"
            target_pod = subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()

            for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                msg = f"{base_id}-{run_idx}"
                log(f"   🔄 [Run {run_idx}/{NUM_REPEAT_TESTS}] Triggering: {msg}")
                
                try:
                    subprocess.run(f"kubectl exec {target_pod} -- python3 start.py --message {msg}", shell=True, check=True)
                    log(f"      ⏳ Propagating ({EXPERIMENT_DURATION}s)...")
                    time.sleep(EXPERIMENT_DURATION + 2)
                    
                    test_summary.append({
                        "test_id": msg, "topology": filename, 
                        "pods": expected_nodes, "timestamp": datetime.now(MYT).strftime('%H:%M:%S')
                    })
                except Exception as e:
                    log(f"      ❌ Trigger Error: {e}")

    finally:
        log("\n" + "="*60)
        log("🧹 FINAL CLEANUP")
        log("="*60)
        try:
            # log("🚮 Cleaning Cloud Shell storage...")
            # subprocess.run(f"gcloud cloud-shell ssh --command='rm -f {REMOTE_PROJECT_DIR}/topology/*.json'", shell=True, capture_output=True)
            
            log("🚮 Uninstalling Helm release...")
            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            
            log(f"🚮 Deleting Cluster {K8SCLUSTER_NAME} (Background)...")
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, "--zone", ZONE, "--project", PROJECT_ID, "--quiet", "--async"], check=False)
            log("✅ Cleanup triggered. Cluster will delete in background.")
        except Exception as e:
            log(f"⚠️ Cleanup error: {e}")

        if test_summary:
            with open(full_csv_path, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["test_id", "topology", "pods", "timestamp"])
                writer.writeheader(); writer.writerows(test_summary)
            log(f"📊 Summary saved: {full_csv_path}")

if __name__ == "__main__":
    main()