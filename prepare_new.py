from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import json
import subprocess
import sys
import os
import time

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
    cmd = ['kubectl', 'get', 'pods', '-l', 'app=bcgossip', '-o', 'jsonpath={range .items[*]}{.metadata.name}{" "}{.status.podIP}{"\\n"}{end}']
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True, timeout=15)
        if not result.stdout.strip(): return False
        pods_data = [line.split() for line in result.stdout.splitlines() if line]
        pods_data.sort(key=lambda x: x[0])
        return [(i, name, ip) for i, (name, ip) in enumerate(pods_data)]
    except Exception: return False

def update_pod_db(pod_name, neighbors):
    """Writes neighbor data to the pod's SQLite DB with internal retry logic."""
    neighbors_json = json.dumps(neighbors)
    
    # We add a retry loop INSIDE the injected script
    python_script = f"""
import sqlite3, json, sys, time, random
try:
    values = json.loads('{neighbors_json.replace("'", "\\'")}')
    success = False
    for i in range(10): # Try up to 10 times
        try:
            with sqlite3.connect('ned.db', timeout=30) as conn:
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('BEGIN IMMEDIATE TRANSACTION') # 'IMMEDIATE' helps prevent deadlocks
                conn.execute('DROP TABLE IF EXISTS NEIGHBORS')
                conn.execute('CREATE TABLE NEIGHBORS (pod_ip TEXT PRIMARY KEY, weight REAL)')
                conn.executemany('INSERT INTO NEIGHBORS VALUES (?, ?)', values)
                conn.commit()
            success = True
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(random.uniform(0.1, 0.5))
                continue
            raise e
    if success:
        print(f"SUCCESS:{{len(values)}}")
    else:
        print("ERROR:Could not acquire lock after retries", file=sys.stderr)
        sys.exit(1)
except Exception as e:
    print(f"ERROR:{{e}}", file=sys.stderr)
    sys.exit(1)
"""
    cmd = ['kubectl', 'exec', pod_name, '--', 'python3', '-c', python_script]
    try:
        # Increased timeout for 500-node density
        result = subprocess.run(cmd, check=True, text=True, capture_output=True, timeout=60)
        return True, result.stdout.strip()
    except Exception as e:
        # Return the stderr so we can see the actual Python error
        error_msg = getattr(e, 'stderr', str(e))
        return False, error_msg

def get_pod_mapping(pod_deployment, pod_neighbors, pod_topology):
    gossip_id_to_ip = {f'gossip-{index}': ip for index, _, ip in pod_deployment}
    edge_weights_lookup = {}
    for edge in pod_topology['edges']:
        s, t, w = str(edge['source']), str(edge['target']), edge['weight']
        edge_weights_lookup[tuple(sorted((s, t)))] = w

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


    """Writes neighbor data directly to the pod's SQLite DB."""
    neighbors_json = json.dumps(neighbors)
    python_script = f"""
import sqlite3, json, sys
try:
    values = json.loads('{neighbors_json.replace("'", "\\'")}')
    with sqlite3.connect('ned.db') as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('BEGIN TRANSACTION')
        conn.execute('DROP TABLE IF EXISTS NEIGHBORS')
        conn.execute('CREATE TABLE NEIGHBORS (pod_ip TEXT PRIMARY KEY, weight REAL)')
        conn.executemany('INSERT INTO NEIGHBORS VALUES (?, ?)', values)
        conn.commit()
    print(f"SUCCESS:{{len(values)}}")
except Exception as e:
    print(f"ERROR:{{e}}", file=sys.stderr)
    sys.exit(1)
"""
    cmd = ['kubectl', 'exec', pod_name, '--', 'python3', '-c', python_script]
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True, timeout=30)
        return True, result.stdout.strip()
    except Exception as e:
        return False, str(e)

def update_all_pods(pod_mapping, max_concurrent=20):
    pod_list = list(pod_mapping.keys())
    total_pods = len(pod_list)
    start_time = time.time()
    success_count = 0

    print(f"\n[DB-Only Update] Injecting topology into {total_pods} pods...", flush=True)

    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        futures = {executor.submit(update_pod_db, p, pod_mapping[p]): p for p in pod_list}
        for i, future in enumerate(as_completed(futures), 1):
            success, output = future.result()
            if success: success_count += 1
            else: print(f"\n  - Failed {futures[future]}: {output}")
            print(f"\rProgress: {(i/total_pods)*100:.1f}% | Success: {success_count}/{total_pods}", end='', flush=True)

    print(f"\n\nOverall Summary - Total Pods: {total_pods}, DB Update Success: {success_count}, Notification Success: {success_count}")
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
            if update_all_pods(pod_map):
                print("Platform is now ready for testing..!")