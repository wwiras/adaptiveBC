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
parser = argparse.ArgumentParser()
parser.add_argument("--k8snodes", type=int, default=3)
parser.add_argument("--p2pnodes", type=int, default=10)
parser.add_argument("--zone", type=str, default="us-central1-c")
parser.add_argument("--project_id", type=str, default="stoked-cosine-415611")
parser.add_argument("--cluster_name", type=str, default="bcgossip-cluster")
args = parser.parse_args()

PROJECT_ID = args.project_id
ZONE = args.zone
K8SCLUSTER_NAME = args.cluster_name
K8SNODE_COUNT = args.k8snodes
P2P_TARGET = args.p2pnodes

IMAGE_TAG = "v19"
TOPOLOGY_FOLDER = "topology"
HELM_CHART_FOLDER = "simcl2"
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
# 🛠️ INTEGRATED EXPERIMENT HELPER
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

    def inject_single_node(self, pod_name, neighbor_data):
        neighbors_json = json.dumps(neighbor_data)
        python_script = f"""
import sqlite3, json, sys
try:
    data = json.loads('{neighbors_json.replace("'", "\\'")}')
    with sqlite3.connect('ned.db') as conn:
        conn.execute('BEGIN TRANSACTION')
        conn.execute('DROP TABLE IF EXISTS NEIGHBORS')
        conn.execute('CREATE TABLE NEIGHBORS (pod_ip TEXT PRIMARY KEY, weight REAL)')
        conn.executemany('INSERT INTO NEIGHBORS VALUES (?, ?)', data)
        conn.commit()
    
    import grpc, gossip_pb2_grpc
    from google.protobuf.empty_pb2 import Empty
    with grpc.insecure_channel('localhost:5050') as chan:
        gossip_pb2_grpc.GossipServiceStub(chan).UpdateNeighbors(Empty(), timeout=10)
    print("SUCCESS")
except Exception as e:
    print(f"ERROR: {{e}}")
    sys.exit(1)
"""
        try:
            self.run_command(['kubectl', 'exec', pod_name, '--', 'python3', '-c', python_script], shell=False)
            return True
        except:
            return False

    
    def push_topology(self, topology_path, pod_details):
        with open(topology_path) as f:
            topo = json.load(f)
        
        # 1. Create a map of ALL possible pods found in K8s
        # ip_map maps 'gossip-0' -> '10.80.x.x'
        ip_map = {f'gossip-{i}': ip for i, (name, ip) in enumerate(pod_details)}
        
        # 2. Build neighbor map
        # We use 'gossip-i' as the universal key format to match ip_map
        neighbor_map = {f'gossip-{i}': [] for i in range(len(pod_details))}
        
        for edge in topo['edges']:
            s = str(edge['source'])
            t = str(edge['target'])
            w = edge.get('weight', 0)

            # Ensure keys are in 'gossip-X' format to match neighbor_map keys
            src_key = s if s.startswith('gossip-') else f'gossip-{s}'
            tgt_key = t if t.startswith('gossip-') else f'gossip-{t}'

            # Add neighbors only if both pods exist in the current deployment
            if src_key in neighbor_map and tgt_key in ip_map:
                neighbor_map[src_key].append((ip_map[tgt_key], w))
            
            # Handle undirected graphs
            if not topo.get('directed', False):
                if tgt_key in neighbor_map and src_key in ip_map:
                    neighbor_map[tgt_key].append((ip_map[src_key], w))

        log(f"💉 Injecting topology into {len(pod_details)} pods (Parallel)...")
        
        # 3. Parallel Injection
        success_count = 0
        # Increased max_workers for larger clusters (>100 nodes)
        workers = min(len(pod_details), 50) 
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # We iterate through pod_details to get the physical name and logical index
            futures = {}
            for i, (pod_name, pod_ip) in enumerate(pod_details):
                logical_id = f'gossip-{i}'
                data = neighbor_map.get(logical_id, []) # Get neighbors or empty list
                futures[executor.submit(self.inject_single_node, pod_name, data)] = pod_name
            
            for future in as_completed(futures):
                if future.result():
                    success_count += 1
                else:
                    failed_name = futures[future]
                    log(f"⚠️ Pod {failed_name} failed topology injection.")
        
        return success_count == len(pod_details)

# ==========================================
# 🚀 MAIN ORCHESTRATOR
# ==========================================
def main():
    helper = ExperimentHelper()
    ROOT_DIR = os.getcwd()
    test_summary = []

    log("\n" + "="*50)
    log(f"🏗️  GKE: {K8SCLUSTER_NAME} | Target: {P2P_TARGET} pods")
    log("="*50 + "\n")

    # 1. Infrastructure Setup
    try:
        subprocess.run(["gcloud", "container", "clusters", "create", K8SCLUSTER_NAME, "--project", PROJECT_ID,
                        "--zone", ZONE, "--num-nodes", str(K8SNODE_COUNT), "--machine-type", "e2-medium", "--quiet"], 
                       check=True, capture_output=True)
        log("✅ Cluster created.")
    except:
        log("ℹ️ Reusing existing cluster.")

    subprocess.run(["gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, "--zone", ZONE, "--project", PROJECT_ID], check=True, capture_output=True)

    # 2. Experiment Loop
    topology_files = sorted(glob.glob(os.path.join(TOPOLOGY_FOLDER, f"*nodes{P2P_TARGET}*.json")))
    
    try:
        for filepath in topology_files:
            filename = os.path.basename(filepath)
            run_uid = ''.join(secrets.choice(string.digits + string.ascii_letters) for _ in range(5))
            base_id = f"{run_uid}-cubaan{P2P_TARGET}"
            log(f"\n🚀 STARTING TOPOLOGY: {filename}")

            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            time.sleep(5)
            os.chdir(HELM_CHART_FOLDER)
            subprocess.run(f"helm install simcn ./chartsim --set totalNodes={P2P_TARGET},image.tag={IMAGE_TAG}", shell=True, check=True, capture_output=True)
            os.chdir(ROOT_DIR)

            log(f"⏳ Waiting for {P2P_TARGET} pods to be 'Running'...")
            while True:
                res = subprocess.run("kubectl get pods -l app=bcgossip --no-headers | grep Running | wc -l", shell=True, capture_output=True, text=True)
                if int(res.stdout.strip() or 0) == P2P_TARGET: break
                time.sleep(5)
            
            pod_details = helper.get_pod_details()
            if not helper.push_topology(filepath, pod_details):
                log("🛑 ABORT: Topology injection failed. Integrity compromised.")
                break

            for run_idx in range(1, 4):
                msg = f"{base_id}-{run_idx}"
                log(f"   🔄 Run {run_idx}: {msg}")
                target_pod = pod_details[0][0] 
                subprocess.run(f"kubectl exec {target_pod} -- python3 start.py --message {msg}", shell=True, check=True)
                time.sleep(5)
                test_summary.append({"test_id": msg, "topology": filename, "pods": P2P_TARGET, "timestamp": datetime.now(MYT).strftime('%H:%M:%S')})

    finally:
        log("\n🧹 STARTING CLEANUP...")
        try:
            # 1. Synchronous Helm Release
            log("🚮 Uninstalling Helm release 'simcn'...")
            subprocess.run(["helm", "uninstall", "simcn"], capture_output=True)
            
            # 2. Wait for pods to fully terminate
            log("⏳ Waiting for all gossip pods to terminate...")
            while True:
                check_pods = subprocess.run("kubectl get pods -l app=bcgossip --no-headers", shell=True, capture_output=True, text=True)
                if not check_pods.stdout.strip():
                    log("✅ All pods cleared.")
                    break
                time.sleep(5)

            # 3. Final Cluster Deletion
            log(f"🚮 Deleting Cluster {K8SCLUSTER_NAME} (Sync)...")
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, "--zone", ZONE, "--project", PROJECT_ID, "--quiet"], check=False)
            log("✅ Cleanup complete.")
        except Exception as e:
            log(f"⚠️ Cleanup error: {e}")

        if test_summary:
            with open(full_csv_path, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["test_id", "topology", "pods", "timestamp"])
                writer.writeheader(); writer.writerows(test_summary)
            log(f"📊 Results saved to {full_csv_path}")

if __name__ == "__main__":
    main()