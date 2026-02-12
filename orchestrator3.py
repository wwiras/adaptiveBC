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
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# 🔧 ARGUMENT PARSING (Dynamic Variables)
# ==========================================
parser = argparse.ArgumentParser()
parser.add_argument("--k8snodes", type=int, default=3)
parser.add_argument("--zone", type=str, default="us-central1-c")
parser.add_argument("--project_id", type=str, default="stoked-cosine-415611")
parser.add_argument("--cluster_name", type=str, default="bcgossip-cluster")
args = parser.parse_args()

# ==========================================
# 🔧 USER CONFIGURATION
# ==========================================
PROJECT_ID = args.project_id
ZONE = args.zone
K8SCLUSTER_NAME = args.cluster_name
K8SNODE_COUNT = args.k8snodes

IMAGE_NAME = "wwiras/simcl2"
IMAGE_TAG = "v22"
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
# 📝 LOGGING & CSV SETUP
# ==========================================
LOG_DIR = "logs"
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
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(full_log_path), logging.StreamHandler()]
)

logging.Formatter.converter = lambda *args: datetime.now(MYT).timetuple()
def log(msg): logging.info(msg)

# ==========================================
# 🛠️ HELPER CLASS (Merged with prepare.py)
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

    def run_command_with_retry(self, cmd, timeout=60, retries=5):
        """Merged from prepare.py - Handles GKE API Server saturation."""
        for attempt in range(retries):
            try:
                result = subprocess.run(cmd, check=True, text=True, capture_output=True, timeout=timeout)
                return True, result.stdout.strip()
            except subprocess.CalledProcessError as e:
                err = e.stderr.strip().lower()
                if "connection" in err or "refused" in err:
                    time.sleep(random.uniform(3.0, 7.0) * (attempt + 1))
                else:
                    time.sleep(1)
            except Exception:
                time.sleep(2)
        return False, "API Server unavailable after retries."

    def get_pod_mapping(self, topology_data):
        """Determines neighbor IP mapping based on deployment vs topology file."""
        cmd = ['kubectl', 'get', 'pods', '-l', 'app=bcgossip', '-o', 'jsonpath={range .items[*]}{.metadata.name}{" "}{.status.podIP}{"\\n"}{end}']
        success, output = self.run_command_with_retry(cmd)
        if not success: return None

        pods = [line.split() for line in output.splitlines() if line]
        pods.sort(key=lambda x: x[0])
        pod_deployment = [(i, name, ip) for i, (name, ip) in enumerate(pods)]

        gossip_id_to_ip = {f'gossip-{index}': ip for index, _, ip in pod_deployment}
        edge_weights = {tuple(sorted((str(e['source']), str(e['target'])))): e['weight'] for e in topology_data['edges']}
        
        neighbor_map = {node['id']: [] for node in topology_data['nodes']}
        for edge in topology_data['edges']:
            s, t = str(edge['source']), str(edge['target'])
            neighbor_map[s].append(t)
            if not topology_data.get('directed', False): neighbor_map[t].append(s)

        mapping = {}
        for index, d_name, _ in pod_deployment:
            g_id = f'gossip-{index}'
            neighbors = []
            for n_id in neighbor_map.get(g_id, []):
                if n_id in gossip_id_to_ip:
                    weight = edge_weights.get(tuple(sorted((g_id, n_id))), 0)
                    neighbors.append((gossip_id_to_ip[n_id], weight))
            mapping[d_name] = neighbors
        return mapping

    def update_pod_db(self, pod_name, neighbors):
        """Injects neighbor data into the remote pod's SQLite DB."""
        neighbors_json = json.dumps(neighbors)
        python_script = f"""
import sqlite3, json, sys, time, random
try:
    values = json.loads('{neighbors_json.replace("'", "\\'")}')
    success = False
    for i in range(10):
        try:
            with sqlite3.connect('ned.db', timeout=30) as conn:
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('BEGIN IMMEDIATE TRANSACTION')
                conn.execute('DROP TABLE IF EXISTS NEIGHBORS')
                conn.execute('CREATE TABLE NEIGHBORS (pod_ip TEXT PRIMARY KEY, weight REAL)')
                conn.executemany('INSERT INTO NEIGHBORS VALUES (?, ?)', values)
                conn.commit()
            success = True
            break
        except sqlite3.OperationalError:
            time.sleep(random.uniform(0.2, 0.8)); continue
    if success: print(f"SUCCESS:{{len(values)}}")
    else: sys.exit(1)
except Exception as e:
    print(f"ERROR:{{e}}", file=sys.stderr); sys.exit(1)
"""
        cmd = ['kubectl', 'exec', pod_name, '--', 'python3', '-c', python_script]
        return self.run_command_with_retry(cmd)

    def inject_topology(self, topology_path, max_concurrent=25):
        """Orchestrates the full injection process for a specific topology."""
        with open(topology_path) as f:
            topo_data = json.load(f)
        
        mapping = self.get_pod_mapping(topo_data)
        if not mapping: return False

        log(f"💉 Injecting topology into {len(mapping)} pods (Concurrency: {max_concurrent})...")
        success_count = 0
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = {executor.submit(self.update_pod_db, p, mapping[p]): p for p in mapping}
            for future in as_completed(futures):
                success, output = future.result()
                if success: success_count += 1
                else: log(f"  - Failed {futures[future]}: {output}")
        
        return success_count == len(mapping)

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
                log(f"✅ Pods are READY ({running_pods}/{expected_pods}).")
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
                log(f"⏱️ Trigger Timeout reached.")
                process.kill(); return False 

            reads = [process.stdout.fileno()]
            ready = select.select(reads, [], [], 1.0)[0]

            if ready:
                line = process.stdout.readline()
                if not line: break 
                if "Received acknowledgment" in line and test_id in line:
                    log(f"✅ VALID ACK RECEIVED!")
                    process.terminate(); return True
            
            if process.poll() is not None: break
        return False

    def wait_for_cleanup(self, namespace='default', timeout=300):
        log("⏳ Ensuring all previous pods are terminated...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            count = self.get_current_running_pod_count(namespace)
            # We check for any pods with the label, even if not 'Running'
            cmd = f"kubectl get pods -n {namespace} -l app=bcgossip --no-headers | wc -l"
            total_pods = int(self.run_command(cmd, suppress_output=True) or 0)
            
            if total_pods == 0:
                log("✅ Environment cleared.")
                return True
            time.sleep(5)
        log("⚠️ Timeout waiting for pod cleanup. Proceeding anyway...")
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
        node_count = int(node_match.group(1))
        topology_list.append({"path": filepath, "filename": filename, "node_count": node_count})

    topology_list.sort(key=lambda x: x['node_count'])
    if not topology_list: return log("❌ No topology files found.")
    
    # 2. INFRASTRUCTURE SETUP
    log("\n" + "="*50 + "\n🏗️ INFRASTRUCTURE CONFIGURATION\n" + "="*50)
    try:
        subprocess.run([
            "gcloud", "container", "clusters", "create", K8SCLUSTER_NAME,
            "--zone", ZONE, "--num-nodes", str(K8SNODE_COUNT), 
            "--machine-type", MTYPE, "--quiet"
        ], check=True, capture_output=True, text=True)
        log("✅ Cluster created.")
    except subprocess.CalledProcessError as e:
        if "already exists" in e.stderr.lower(): log("ℹ️ Cluster exists. Fetching credentials...")
        else: log(f"❌ CRITICAL ERROR: {e.stderr}"); sys.exit(1) 

    subprocess.run(["gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME, "--zone", ZONE, "--project", PROJECT_ID], check=True)

    # 3. EXPERIMENT LOOP
    try:
        for i, topo in enumerate(topology_list):
            filename = topo['filename']
            p2p_nodes = topo['node_count']
            base_test_id = f"{get_short_id(5)}-cubaan{p2p_nodes}"

            log(f"\n[{i+1}/{len(topology_list)}] 🚀 TOPOLOGY: {filename}")
            
            # --- A. CONDITIONAL HELM DEPLOYMENT ---
            current_workload = helper.get_current_running_pod_count()
            if current_workload != p2p_nodes:
                log(f"🔄 Scaling pods from {current_workload} to {p2p_nodes}...")
                try:
                    # 1. Trigger uninstall
                    helper.run_command("helm uninstall simcn", suppress_output=True)
                    log("🗑️ Helm uninstall triggered.")
                except: 
                    pass

                # 2. Wait for pods to actually disappear
                helper.wait_for_cleanup()

                # 3. Fresh Install
                os.chdir(HELM_CHART_FOLDER)
                try:
                    helm_cmd = (f"helm install simcn ./chartsim --set testType=default,"
                                f"totalNodes={p2p_nodes},image.tag={IMAGE_TAG},image.name={IMAGE_NAME}")
                    helper.run_command(helm_cmd, capture=False)
                finally:
                    os.chdir(ROOT_DIR)

                # 4. Wait for new pods to be ready
                if not helper.wait_for_pods_to_be_ready(expected_pods=p2p_nodes):
                    raise Exception("Pods scale-up failed.")
            
            # --- B. INJECT TOPOLOGY (Native Call) ---
            if not helper.inject_topology(topo['path']):
                log("⚠️ Topology injection failed. Skipping this topology.")
                continue

            test_summary.append({
                "test_id": base_test_id, "topology": filename, "pods": p2p_nodes,
                "timestamp": datetime.now(MYT).strftime('%Y-%m-%d %H:%M:%S')
            })

            # --- C. REPEAT TEST LOOP ---
            for run_idx in range(1, NUM_REPEAT_TESTS + 1):
                message = f"{base_test_id}-{run_idx}"
                pod = helper.select_random_pod()
                helper.trigger_gossip_hybrid(pod, message, cycle_index=run_idx)
                log(f"      ⏳ Propagating ({EXPERIMENT_DURATION}s)...")
                time.sleep(EXPERIMENT_DURATION + 2)

    except Exception as e: log(f"❌ CRITICAL ERROR: {e}")
    
    finally:
        log("\n🧹 Cleanup...")
        try:
            subprocess.run(["helm", "uninstall", "simcn"], check=False)
            subprocess.run(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, "--zone", ZONE, "--quiet"], check=False)
        except: pass

        # FINAL CSV EXPORT
        log("\n" + "="*80 + "\n📋 FINAL TEST SUMMARY\n" + "="*80)
        with open(full_csv_path, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["test_id", "topology", "pods", "timestamp"])
            writer.writeheader()
            for entry in test_summary:
                log(f"{entry['test_id']:<30} | {entry['topology']:<40} | {entry['pods']:<5}")
                writer.writerow(entry)
        log("🏁 Done.")

if __name__ == "__main__":
    main()