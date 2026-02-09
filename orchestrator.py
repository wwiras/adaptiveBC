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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
parser.add_argument("--k8s_nodes", type=int, default=3, help="GKE worker nodes")
parser.add_argument("--p2p_nodes", type=int, default=10, help="Target P2P pods")
parser.add_argument("--autoscale", type=str2bool, default=False, help="Enable GKE autoscaling")
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

full_log_path = os.path.join(LOG_DIR, f"orchestrator_{timestamp_str}_{unique_run_id}.log")
full_csv_path = os.path.join(LOG_DIR, f"orchestrator_{timestamp_str}_{unique_run_id}.csv")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S",
                    handlers=[logging.FileHandler(full_log_path), logging.StreamHandler()])
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

    def get_pod_details(self):
        cmd = "kubectl get pods -l app=bcgossip -o jsonpath='{range .items[*]}{.metadata.name}{\" \"}{.status.podIP}{\"\\n\"}{end}'"
        stdout = self.run_command(cmd)
        pods = [line.split() for line in stdout.splitlines() if line]
        pods.sort(key=lambda x: x[0])
        return pods 

    def inject_to_single_pod(self, pod_name, neighbor_data, retries=3):
        """Phase 1: SQLite Transaction; Phase 2: gRPC Notify. Includes Retry Loop."""
        neighbors_json = json.dumps(neighbor_data)
        db_script = f"""
import sqlite3, json, sys
try:
    data = json.loads('{neighbors_json}')
    with sqlite3.connect('ned.db') as conn:
        conn.execute('BEGIN TRANSACTION')
        conn.execute('DROP TABLE IF EXISTS NEIGHBORS')
        conn.execute('CREATE TABLE NEIGHBORS (pod_ip TEXT PRIMARY KEY, weight REAL)')
        conn.executemany('INSERT INTO NEIGHBORS VALUES (?, ?)', data)
        conn.commit()
except Exception: sys.exit(1)
"""
        notify_script = "import grpc, gossip_pb2_grpc, sys; from google.protobuf.empty_pb2 import Empty; \
                         try: \
                             with grpc.insecure_channel('localhost:5050') as chan: \
                                 gossip_pb2_grpc.GossipServiceStub(chan).UpdateNeighbors(Empty(), timeout=10) \
                         except Exception: sys.exit(1)"
        
        for attempt in range(1, retries + 1):
            try:
                self.run_command(['kubectl', 'exec', pod_name, '--', 'python3', '-c', db_script], shell=False)
                self.run_command(['kubectl', 'exec', pod_name, '--', 'python3', '-c', notify_script], shell=False)
                return True, pod_name
            except Exception as e:
                if attempt < retries:
                    # Random jitter for retries to avoid API congestion
                    wait = random.uniform(1.0, 3.0)
                    time.sleep(wait)
                else:
                    return False, pod_name

    def push_topology_parallel(self, topology_path, pod_details):
        log(f"💉 Injecting topology into {len(pod_details)} pods (Parallel + Retry)...")
        start_time = time.time()
        
        with open(topology_path) as f:
            topo = json.load(f)
        
        ip_map = {f'gossip-{i}': ip for i, (name, ip) in enumerate(pod_details)}
        neighbor_map = {node['id']: [] for node in topo['nodes']}
        for edge in topo['edges']:
            neighbor_map[edge['source']].append((ip_map[edge['target']], edge['weight']))
            if not topo['directed']:
                neighbor_map[edge['target']].append((ip_map[edge['source']], edge['weight']))

        failed_pods = []
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(self.inject_to_single_pod, name, neighbor_map[f'gossip-{i}']) 
                       for i, (name, ip) in enumerate(pod_details)]
            for future in as_completed(futures):
                success, pod_name = future.result()
                if not success: failed_pods.append(pod_name)
        
        if failed_pods:
            log(f"❌ INJECTION FAILED after retries for: {failed_pods}")
            return False
        
        log(f"✅ Injection Successful in {time.time()-start_time:.2f}s")
        return True

    def trigger_gossip_hybrid(self, pod_name, test_id, cycle):
        timeout = BASE_TRIGGER_TIMEOUT + ((cycle - 1) * TIMEOUT_INCREMENT)
        cmd = ['kubectl', 'exec', pod_name, '--', 'python3', 'start.py', '--message', test_id]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        start = time.time()
        while time.time() - start < timeout:
            r, _, _ = select.select([proc.stdout], [], [], 1.0)
            if r:
                line = proc.stdout.readline()
                if "Received acknowledgment" in line and test_id in line:
                    log(f"      ✅ VALID ACK RECEIVED for {test_id}!")
                    proc.terminate(); return True
            if proc.poll() is not None: break
        proc.kill(); return False

# ==========================================
# 🚀 MAIN ORCHESTRATOR
# ==========================================
def main():
    helper = ExperimentHelper()
    ROOT_DIR = os.getcwd()
    test_summary = []

    log("\n" + "="*50)
    log("🏗️  INFRASTRUCTURE CONFIGURATION")
    log(f"   - Project: {PROJECT_ID} | Zone: {ZONE}")
    log(f"   - Cluster: {K8SCLUSTER_NAME} | Target: {P2P_TARGET} pods")
    log("="*50 + "\n")

    # Cluster Sync Setup
    check_cmd = f"gcloud container clusters list --project {PROJECT_ID} --filter='name:{K8SCLUSTER_NAME}' --format='value(name)' --zone {ZONE}"
    existing = subprocess.run(check_cmd, shell=True, capture_output=True, text=True).stdout.strip()

    if not existing:
        log(f"🔨 Creating cluster...")
        create_cmd = ["gcloud", "container", "clusters", "create", K8SCLUSTER_NAME, "--project", PROJECT_ID,
                      "--zone", ZONE, "--num-nodes", str(K8S_NODES), "--machine-type", "e2-medium", "--enable-ip-alias", "--quiet"]
        if AUTOSCALE_ENABLED: create_cmd.extend(["--enable-autoscaling", "--min-nodes", "1", "--max-nodes", "100"])
        subprocess.run(create_cmd, check=True)
    else:
        log(f"ℹ️ Reusing existing cluster.")

    subprocess.run(["gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, "--zone", ZONE, "--project", PROJECT_ID], check=True, capture_output=True)

    # 2. EXPERIMENT LOOP
    topology_files = sorted(glob.glob(os.path.join(TOPOLOGY_FOLDER, f"*nodes{P2P_TARGET}*.json")))
    
    try:
        for filepath in topology_files:
            filename = os.path.basename(filepath)
            unique_id = ''.join(secrets.choice(string.digits + string.ascii_letters) for _ in range(5))
            base_id = f"{unique_id}-cubaan{P2P_TARGET}"
            log(f"\n🚀 STARTING: {filename}")

            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            time.sleep(5)
            os.chdir(HELM_CHART_FOLDER)
            subprocess.run(f"helm install simcn ./chartsim --set totalNodes={P2P_TARGET},image.tag={IMAGE_TAG}", shell=True, check=True, capture_output=True)
            os.chdir(ROOT_DIR)

            log("⏳ Waiting for pods to reach 'Running'...")
            while int(helper.run_command("kubectl get pods -l app=bcgossip --no-headers | grep Running | wc -l")) < P2P_TARGET:
                time.sleep(5)
            
            pods = helper.get_pod_details()
            if not helper.push_topology_parallel(filepath, pods):
                log(f"🛑 ABORTING: Injection failed for {filename}.")
                break 

            for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                msg = f"{base_id}-{run_idx}"
                log(f"   🔄 Run {run_idx}/{NUM_REPEAT_TESTS}: {msg}")
                pod = helper.run_command("kubectl get pods -l app=bcgossip -o name | head -n 1").split('/')[-1]
                helper.trigger_gossip_hybrid(pod, msg, run_idx)
                time.sleep(EXPERIMENT_DURATION + 1)
                test_summary.append({"test_id": msg, "topology": filename, "pods": P2P_TARGET, "timestamp": datetime.now(MYT).strftime('%H:%M:%S')})

    except Exception as e:
        log(f"❌ CRITICAL ERROR: {e}")
    finally:
        # --- SYNCHRONOUS CLEANUP ---
        log("\n🧹 STARTING CLEANUP: Waiting for resource termination...")
        try:
            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            
            # Synchronous wait for pods to disappear
            log("⏳ Waiting for all pods to terminate...")
            while True:
                cmd = "kubectl get pods -l app=bcgossip --no-headers"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if not result.stdout.strip():
                    log("✅ All pods terminated.")
                    break
                time.sleep(5)

            log("🚮 Deleting GKE Cluster...")
            # We remove --async here to ensure the script stays alive until the cluster is gone
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, 
                            "--zone", ZONE, "--project", PROJECT_ID, "--quiet"], check=False)
            log("✅ Cluster deleted.")
        except Exception as e:
            log(f"⚠️ Cleanup warning: {e}")

        if test_summary:
            with open(full_csv_path, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["test_id", "topology", "pods", "timestamp"])
                writer.writeheader(); writer.writerows(test_summary)
            log(f"📊 CSV saved: {full_csv_path}")
        log("🏁 Done.")

if __name__ == "__main__":
    main()