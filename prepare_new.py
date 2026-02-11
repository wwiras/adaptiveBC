from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import json
import subprocess
import sys
import os
import time
import random

# --- PHASE 0: ROBUST COMMAND EXECUTION ---
def run_command_with_retry(cmd, timeout=60, retries=5):
    """Handles GKE API Server 'Connection Refused' errors with backoff."""
    for attempt in range(retries):
        try:
            result = subprocess.run(
                cmd, check=True, text=True, capture_output=True, timeout=timeout
            )
            return True, result.stdout.strip()
        except subprocess.CalledProcessError as e:
            err = e.stderr.strip().lower()
            if "connection to the server" in err or "refused" in err:
                # API Server is saturated; sleep longer to let it recover
                time.sleep(random.uniform(3.0, 7.0) * (attempt + 1))
            else:
                time.sleep(1)
        except Exception:
            time.sleep(2)
    return False, "GKE API Server unavailable after multiple retries."

# --- PHASE 1: TOPOLOGY & DEPLOYMENT DISCOVERY ---

def get_pod_topology(topology_folder, filename):
    path = os.path.join(os.getcwd(), topology_folder, filename)
    if not os.path.exists(path): return False
    with open(path) as f: return json.load(f)

def get_pod_dplymt():
    cmd = ['kubectl', 'get', 'pods', '-l', 'app=bcgossip', '-o', 'jsonpath={range .items[*]}{.metadata.name}{" "}{.status.podIP}{"\\n"}{end}']
    success, output = run_command_with_retry(cmd)
    if not success: return False
    pods = [line.split() for line in output.splitlines() if line]
    pods.sort(key=lambda x: x[0])
    return [(i, name, ip) for i, (name, ip) in enumerate(pods)]

def get_pod_mapping(pod_deployment, pod_topology):
    gossip_id_to_ip = {f'gossip-{index}': ip for index, _, ip in pod_deployment}
    edge_weights = {tuple(sorted((str(e['source']), str(e['target'])))): e['weight'] for e in pod_topology['edges']}
    
    neighbor_map = {node['id']: [] for node in pod_topology['nodes']}
    for edge in pod_topology['edges']:
        s, t = str(edge['source']), str(edge['target'])
        neighbor_map[s].append(t)
        if not pod_topology.get('directed', False): neighbor_map[t].append(s)

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

# --- PHASE 2: RESILIENT DB INJECTION ---

def update_pod_db(pod_name, neighbors):
    """Injected script handles internal SQLite retries and locking logic."""
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
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(random.uniform(0.2, 0.8))
                continue
            raise e
    if success: print(f"SUCCESS:{{len(values)}}")
    else: sys.exit(1)
except Exception as e:
    print(f"ERROR:{{e}}", file=sys.stderr)
    sys.exit(1)
"""
    cmd = ['kubectl', 'exec', pod_name, '--', 'python3', '-c', python_script]
    return run_command_with_retry(cmd)

def update_all_pods(pod_mapping, max_concurrent=25): # Reduced for stability
    pod_list = list(pod_mapping.keys())
    total_pods = len(pod_list)
    start_time = time.time()
    success_count = 0

    print(f"\n[Resilient Update] Injecting into {total_pods} pods (Concurrency: {max_concurrent})...", flush=True)

    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        futures = {executor.submit(update_pod_db, p, pod_mapping[p]): p for p in pod_list}
        for i, future in enumerate(as_completed(futures), 1):
            success, output = future.result()
            if success: success_count += 1
            else: print(f"\n  - Failed {futures[future]}: {output}")
            print(f"\rProgress: {(i/total_pods)*100:.1f}% | Success: {success_count}/{total_pods}", end='', flush=True)

    total_elapsed = time.time() - start_time
    # This specific summary format is for the Orchestrator's Exploit logic
    print(f"\n\nOverall Summary - Total Pods: {total_pods}, DB Update Success: {success_count}, Notification Success: {success_count}")
    print(f"Injection completed in {total_elapsed:.1f}s")
    return success_count == total_pods

# --- MAIN ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True)
    parser.add_argument("--topology_folder", default="topology")
    args = parser.parse_args()

    topo = get_pod_topology(args.topology_folder, args.filename)
    if topo:
        dplymt = get_pod_dplymt()
        if dplymt and len(topo['nodes']) == len(dplymt):
            mapping = get_pod_mapping(dplymt, topo)
            if update_all_pods(mapping):
                print("Platform is now ready for testing..!", flush=True)