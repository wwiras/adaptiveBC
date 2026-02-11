from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import json
import subprocess
import sys
import os
import time
import random

# --- HELPER: Robust Command Execution ---
def run_command_with_retry(cmd, timeout=60, retries=3, backoff=1.5):
    last_error = ""
    for attempt in range(retries):
        try:
            result = subprocess.run(
                cmd, check=True, text=True, capture_output=True, timeout=timeout
            )
            return True, result.stdout.strip()
        except subprocess.CalledProcessError as e:
            last_error = f"Exit {e.returncode}: {e.stderr.strip()}"
        except subprocess.TimeoutExpired:
            last_error = f"Timed out after {timeout}s"
        except Exception as e:
            last_error = f"Unexpected error: {str(e)}"

        if attempt < retries - 1:
            sleep_time = (backoff ** attempt) + random.uniform(0.5, 1.5)
            time.sleep(sleep_time)
    return False, f"Failed after {retries} attempts. Last error: {last_error}"

# --- TOPOLOGY PARSING ---

def get_pod_topology(topology_folder, filename):
    topology_file_path = os.path.join(os.getcwd(), topology_folder, filename)
    if not os.path.exists(topology_file_path):
        print(f"Error: Topology file not found at '{topology_file_path}'.", flush=True)
        sys.exit(1)
    try:
        with open(topology_file_path) as f:
            return json.load(f)
    except Exception:
        return False

def get_pod_neighbors(topology):
    neighbor_map = {node['id']: [] for node in topology['nodes']}
    for edge in topology['edges']:
        source, target = edge['source'], edge['target']
        neighbor_map[source].append(target)
        if not topology.get('directed', False):
            neighbor_map[target].append(source)
    return neighbor_map

def get_pod_dplymt():
    cmd = [
        'kubectl', 'get', 'pods', '-l', 'app=bcgossip',
        '-o', 'jsonpath={range .items[*]}{.metadata.name}{" "}{.status.podIP}{"\\n"}{end}'
    ]
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True, timeout=15)
        if not result.stdout.strip(): return False
        pods_data = [line.split() for line in result.stdout.splitlines() if line]
        pods_data.sort(key=lambda x: x[0])
        return [(i, name, ip) for i, (name, ip) in enumerate(pods_data)]
    except Exception:
        return False

def get_pod_mapping(pod_deployment, pod_neighbors, pod_topology):
    gossip_id_to_ip = {f'gossip-{index}': ip for index, _, ip in pod_deployment}
    edge_weights_lookup = {}
    for edge in pod_topology['edges']:
        s, t, w = edge['source'], edge['target'], edge['weight']
        edge_weights_lookup[tuple(sorted((str(s), str(t))))] = w

    result = {}
    for index, deployment_name, _ in pod_deployment:
        gossip_id = f'gossip-{index}'
        list_with_weights = []
        if gossip_id in pod_neighbors:
            for n_id in pod_neighbors[gossip_id]:
                if n_id in gossip_id_to_ip:
                    ip = gossip_id_to_ip[n_id]
                    weight = edge_weights_lookup.get(tuple(sorted((gossip_id, n_id))), 0)
                    list_with_weights.append((ip, weight))
        result[deployment_name] = list_with_weights
    return result

# --- INJECTION LOGIC ---

def update_pod_neighbors_db(pod_name, neighbors):
    """Phase 1: Update SQLite DB inside the pod."""
    neighbors_json = json.dumps(neighbors)
    python_script = f"""
import sqlite3, json, sys
try:
    values = json.loads('{neighbors_json.replace("'", "\\'")}')
    with sqlite3.connect('ned.db') as conn:
        conn.execute('BEGIN TRANSACTION')
        conn.execute('DROP TABLE IF EXISTS NEIGHBORS')
        conn.execute('CREATE TABLE NEIGHBORS (pod_ip TEXT PRIMARY KEY, weight REAL)')
        conn.executemany('INSERT INTO NEIGHBORS VALUES (?, ?)', values)
        conn.commit()
    print(f"Updated {{len(values)}} neighbors")
except Exception as e:
    print(f"Error: {{e}}", file=sys.stderr)
    sys.exit(1)
"""
    cmd = ['kubectl', 'exec', pod_name, '--', 'python3', '-c', python_script]
    return run_command_with_retry(cmd)

def notify_pod_for_update_grpc(pod_name):
    """Phase 2: Notify node_asyncio.py to refresh its cache."""
    python_script = """
import grpc, gossip_pb2_grpc
from google.protobuf.empty_pb2 import Empty
import sys
try:
    with grpc.insecure_channel('localhost:5050') as channel:
        stub = gossip_pb2_grpc.GossipServiceStub(channel)
        stub.UpdateNeighbors(Empty(), timeout=10)
    print("Success")
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
"""
    cmd = ['kubectl', 'exec', pod_name, '--', 'python3', '-c', python_script]
    return run_command_with_retry(cmd)

def update_pod_full_flow(pod_name, neighbors):
    """Combines DB and gRPC phases."""
    success, msg = update_pod_neighbors_db(pod_name, neighbors)
    if not success: return False, msg
    
    success, msg = notify_pod_for_update_grpc(pod_name)
    if not success: return False, msg
    
    return True, "Synchronized"

def update_all_pods(pod_mapping, max_concurrent=50):
    pod_list = list(pod_mapping.keys())
    total_pods = len(pod_list)
    start_time = time.time()
    success_count = 0

    print(f"\n[Two-Phase Update] Pushing topology to {total_pods} pods...", flush=True)
    

    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        futures = {executor.submit(update_pod_full_flow, p, pod_mapping[p]): p for p in pod_list}
        for i, future in enumerate(as_completed(futures), 1):
            success, output = future.result()
            if success: success_count += 1
            else: print(f"\n  - Failed {futures[future]}: {output}")
            
            print(f"\rProgress: {(i/total_pods)*100:.1f}% | Success: {success_count}/{total_pods}", end='', flush=True)

    print(f"\n\nTotal Time: {time.time() - start_time:.1f}s")
    return success_count == total_pods

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True)
    parser.add_argument("--topology_folder", default="topology")
    args = parser.parse_args()

    topo = get_pod_topology(args.topology_folder, args.filename)
    if topo:
        dplymt = get_pod_dplymt()
        if dplymt and len(topo['nodes']) == len(dplymt):
            pod_map = get_pod_mapping(dplymt, get_pod_neighbors(topo), topo)
            if pod_map and update_all_pods(pod_map):
                print("Overall Summary - Total Pods: {}, DB Update Success: {}, Notification Success: {}".format(
                    len(dplymt), len(dplymt), len(dplymt)
                ))
                print("Platform is now ready for testing..!", flush=True)
            else:
                print("Update failed on some pods.", flush=True)
        else:
            print(f"Error: Size mismatch. Topology: {len(topo['nodes'])}, Deployment: {len(dplymt) if dplymt else 0}")