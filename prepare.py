from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import json
import subprocess
import sys
import os
import time
import random  # Added for jitter in retries

# --- HELPER: Robust Command Execution ---
def run_command_with_retry(cmd, timeout=300, retries=5, backoff=1.5):
    """
    Executes a subprocess command with retries, exponential backoff, and random jitter.
    This handles transient 'Connection refused' errors from the K8s API server.
    """
    last_error = ""
    
    for attempt in range(retries):
        try:
            # Run the command
            result = subprocess.run(
                cmd, check=True, text=True, capture_output=True, timeout=timeout
            )
            return True, result.stdout.strip()

        except subprocess.CalledProcessError as e:
            # Capture error (e.g., Connection Refused, Exit 1)
            last_error = f"Exit {e.returncode}: {e.stderr.strip()}"
        except subprocess.TimeoutExpired:
            last_error = f"Timed out after {timeout}s"
        except Exception as e:
            last_error = f"Unexpected error: {str(e)}"

        # If we are here, the attempt failed. Wait before retrying.
        # Formula: (Backoff ^ Attempt) + Random Jitter (0.5 - 1.5s)
        # This prevents "thundering herd" where all threads retry at once.
        if attempt < retries - 1:
            sleep_time = (backoff ** attempt) + random.uniform(0.5, 1.5)
            # print(f"   [Retry] Retrying in {sleep_time:.2f}s due to: {last_error[:100]}...", flush=True) # Optional debug
            time.sleep(sleep_time)

    # If all retries fail
    return False, f"Failed after {retries} attempts. Last error: {last_error}"

# --- CORE FUNCTIONS ---

def get_pod_topology(topology_folder, filename):
    """
    Function : It will read the topology (from a given json file - network topology)
    Input: Topology folder name and filename
    Returns: topology object - if found. False, if not found
    """
    topology_file_path = os.path.join(os.getcwd(), topology_folder, filename)

    if not os.path.exists(topology_file_path):
        print(f"Error: Topology file not found at '{topology_file_path}'. Exiting.", flush=True)
        sys.exit(1)

    try:
        with open(topology_file_path) as f:
            topology = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from file '{topology_file_path}'. Exiting.", flush=True)
        topology = False

    return topology


def get_pod_neighbors(topology):
    """
    Creates a dictionary mapping each node to its neighbors.
    """
    neighbor_map = {node['id']: [] for node in topology['nodes']}

    for edge in topology['edges']:
        source = edge['source']
        target = edge['target']

        neighbor_map[source].append(target)
        if not topology['directed']:
            neighbor_map[target].append(source)

    return neighbor_map


def get_pod_dplymt():
    """
    Fetches [(index, pod_name, pod_ip)] from Kubernetes or returns False on failure.
    Uses retries implicitly via subprocess (though simple read usually safe, added basic retry here could be good too).
    """
    cmd = [
        'kubectl',
        'get', 'pods',
        '-l', 'app=bcgossip',
        '-o', 'jsonpath={range .items[*]}{.metadata.name}{" "}{.status.podIP}{"\\n"}{end}'
    ]

    try:
        result = subprocess.run(
            cmd,
            check=True,
            text=True,
            capture_output=True,
            timeout=10
        )

        if not result.stdout.strip():
            print("Error: No pods found with label app=bcgossip")
            return False

        pods_data = [line.split() for line in result.stdout.splitlines() if line]
        pods_data.sort(key=lambda x: x[0])

        return [(i, name, ip) for i, (name, ip) in enumerate(pods_data)]

    except subprocess.CalledProcessError as e:
        print(f"kubectl failed (exit {e.returncode}): {e.stderr.strip()}")
        return False
    except subprocess.TimeoutExpired:
        print("Error: kubectl command timed out after 10 seconds")
        return False
    except Exception as e:
        print(f"Unexpected error in get_pod_dplymt: {str(e)}")
        return False


def get_pod_mapping(pod_deployment, pod_neighbors, pod_topology):
    """
    Creates {deployment_pod_name: [(neighbor_ip, weight), ...]} mapping
    by incorporating edge weights from the topology.
    """
    gossip_id_to_ip = {f'gossip-{index}': ip for index, _, ip in pod_deployment}
    edge_weights_lookup = {}
    for edge in pod_topology['edges']:
        source_id = edge['source']
        target_id = edge['target']
        weight = edge['weight']
        if source_id < target_id:
            edge_weights_lookup[(source_id, target_id)] = weight
        else:
            edge_weights_lookup[(target_id, source_id)] = weight

    result = {}
    for index, deployment_name, _ in pod_deployment:
        gossip_id = f'gossip-{index}'
        if gossip_id in pod_neighbors:
            neighbor_list_with_weights = []
            for neighbor_gossip_id in pod_neighbors[gossip_id]:
                if neighbor_gossip_id in gossip_id_to_ip:
                    neighbor_ip = gossip_id_to_ip[neighbor_gossip_id]
                    if gossip_id < neighbor_gossip_id:
                        edge_key = (gossip_id, neighbor_gossip_id)
                    else:
                        edge_key = (neighbor_gossip_id, gossip_id)
                    weight = edge_weights_lookup.get(edge_key, 0)
                    neighbor_list_with_weights.append((neighbor_ip, weight))
            result[deployment_name] = neighbor_list_with_weights
    return result


def get_num_nodes(namespace='default'):
    """
    Dynamically determines the number of nodes (pods) by counting running pods.
    """
    get_pods_cmd = f"kubectl get pods -n {namespace} --no-headers | grep Running | wc -l"
    try:
        result = subprocess.run(get_pods_cmd, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        num_nodes = int(result.stdout.strip())
        return num_nodes
    except subprocess.CalledProcessError as e:
        print(f"Error getting number of pods: {e.stderr}", flush=True)
        return False


def update_pod_neighbors(pod, neighbors, timeout=300):
    """
    Atomically updates neighbor list (IP and weight) in a pod's SQLite DB.
    Uses robust retry logic.
    """
    neighbors_json = json.dumps(neighbors)
    # We use sys.exit(1) inside python script to ensure subprocess sees failure
    python_script = f"""
import sqlite3
import json
import sys
try:
    values_from_json = json.loads('{neighbors_json.replace("'", "\\'")}')
    with sqlite3.connect('ned.db') as conn:
        conn.execute('BEGIN TRANSACTION')
        conn.execute('DROP TABLE IF EXISTS NEIGHBORS')
        conn.execute('CREATE TABLE NEIGHBORS (pod_ip TEXT PRIMARY KEY, weight REAL)')
        conn.executemany('INSERT INTO NEIGHBORS VALUES (?, ?)', values_from_json)
        conn.commit()
    print(f"Updated {{len(values_from_json)}} neighbors")
except Exception as e:
    print(f"Error: {{str(e)}}", file=sys.stderr)
    sys.exit(1)
"""
    cmd = [
        'kubectl', 'exec', pod,
        '--', 'python3', '-c', python_script
    ]
    
    return run_command_with_retry(cmd, timeout=timeout, retries=5)


def notify_pod_for_update(pod_name):
    """
    Sends a signal to a pod via a kubectl exec command to trigger a neighbor list update.
    Uses robust retry logic.
    """
    python_script = """
import grpc
import gossip_pb2_grpc
import sys
from google.protobuf.empty_pb2 import Empty
try:
    with grpc.insecure_channel('localhost:5050') as channel:
        stub = gossip_pb2_grpc.GossipServiceStub(channel)
        stub.UpdateNeighbors(Empty(), timeout=5)
    print("UpdateNeighbors signal sent.")
except grpc.RpcError as e:
    print(f"RPC Error: {e.code()}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"Error: {str(e)}", file=sys.stderr)
    sys.exit(1)
"""
    cmd = [
        'kubectl', 'exec', pod_name,
        '--', 'python3', '-c', python_script
    ]

    return run_command_with_retry(cmd, timeout=20, retries=5)


def update_all_pods(pod_mapping, pod_dplymt, max_retries=3, initial_timeout=300, max_concurrent_updates=50):
    """
    Performs a two-phase update on all pods with detailed progress monitoring.
    Phase 1: DB Update
    Phase 2: gRPC Notify
    """
    pod_list = list(pod_mapping.keys())
    total_pods = len(pod_list)
    db_update_results = {}
    notify_update_results = {}
    start_time = time.time()

    # --- PHASE 1: PARALLEL DB UPDATE ---
    print(f"\n[Phase 1: DB Update] Starting update for {total_pods} pods (concurrent: {max_concurrent_updates})...",
          flush=True)
    with ThreadPoolExecutor(max_workers=max_concurrent_updates) as executor:
        db_futures = {
            executor.submit(update_pod_neighbors, pod, pod_mapping.get(pod, []), initial_timeout): pod
            for pod in pod_list
        }
        completed_db_updates = 0
        db_success_count = 0
        for future in as_completed(db_futures):
            pod_name = db_futures[future]
            completed_db_updates += 1
            try:
                success, output = future.result()
                db_update_results[pod_name] = (success, output)
                if success:
                    db_success_count += 1
                else:
                    print(f"\n  - DB update failed for {pod_name}: {output}", flush=True)
            except Exception as exc:
                db_update_results[pod_name] = (False, f"Exception: {exc}")
                print(f"\n  - DB update failed for {pod_name} with exception: {exc}", flush=True)

            elapsed = time.time() - start_time
            progress = (completed_db_updates / total_pods) * 100
            print(
                f"\r[Phase 1: DB Update] Progress: {progress:.1f}% | Elapsed: {elapsed:.1f}s | Completed: {completed_db_updates}/{total_pods} | Success: {db_success_count}",
                end='', flush=True
            )

    db_failure_count = total_pods - db_success_count
    print(
        f"\nDB update phase complete in {time.time() - start_time:.1f} seconds. Success: {db_success_count}, Failed: {db_failure_count}",
        flush=True)

    if db_success_count == 0:
        print("No successful DB updates. Skipping gRPC notification phase.", flush=True)
        return False

    # --- PHASE 2: PARALLEL gRPC NOTIFICATION ---
    pods_to_notify = [pod_name for pod_name, (success, _) in db_update_results.items() if success]
    pods_to_notify_count = len(pods_to_notify)
    print(f"\n[Phase 2: gRPC Notify] Starting notification for {pods_to_notify_count} pods...", flush=True)
    with ThreadPoolExecutor(max_workers=max_concurrent_updates) as executor:
        notify_futures = {
            executor.submit(notify_pod_for_update, pod_name): pod_name
            for pod_name in pods_to_notify
        }
        completed_notifications = 0
        notify_success_count = 0
        for future in as_completed(notify_futures):
            pod_name = notify_futures[future]
            completed_notifications += 1
            try:
                success, output = future.result()
                notify_update_results[pod_name] = (success, output)
                if success:
                    notify_success_count += 1
                else:
                    print(f"\n  - Notification failed for {pod_name}: {output}", flush=True)
            except Exception as exc:
                notify_update_results[pod_name] = (False, f"Exception: {exc}")
                print(f"\n  - Notification failed for {pod_name} with exception: {exc}", flush=True)

            elapsed = time.time() - start_time
            progress = (completed_notifications / pods_to_notify_count) * 100
            print(
                f"\r[Phase 2: gRPC Notify] Progress: {progress:.1f}% | Elapsed: {elapsed:.1f}s | Completed: {completed_notifications}/{pods_to_notify_count} | Success: {notify_success_count}",
                end='', flush=True
            )

    notify_failure_count = pods_to_notify_count - notify_success_count
    print(f"\nNotification phase complete. Success: {notify_success_count}, Failed: {notify_failure_count}", flush=True)

    total_time = time.time() - start_time
    print(f"\nTotal process completed in {total_time:.1f} seconds.", flush=True)
    print(
        f"Overall Summary - Total Pods: {total_pods}, DB Update Success: {db_success_count}, Notification Success: {notify_success_count}",
        flush=True)

    return notify_success_count == total_pods


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get pod mapping and neighbor info based on topology.")
    parser.add_argument("--filename", help="Name of the topology JSON file in the 'topology' folder.")
    parser.add_argument("--topology_folder", default="topology", help="Name of the topology folder from the root.")
    args = parser.parse_args()

    prepare = False

    pod_topology = get_pod_topology(args.topology_folder, args.filename)

    if pod_topology:
        nodes_dplymt_list = get_pod_dplymt()

        if not nodes_dplymt_list:
            sys.exit(1)

        nodes_topology = len(pod_topology['nodes'])
        nodes_dplymt_count = len(nodes_dplymt_list)

        if nodes_topology == nodes_dplymt_count and nodes_topology > 0:
            print(f"Deployment number of nodes equal to topology nodes: {nodes_topology}", flush=True)

            pod_neighbors = get_pod_neighbors(pod_topology)
            pod_mapping = get_pod_mapping(nodes_dplymt_list, pod_neighbors, pod_topology)

            if pod_mapping:
                # Increased concurrency to 50 for speed, relying on retry logic for robustness
                if update_all_pods(pod_mapping, nodes_dplymt_list, max_concurrent_updates=50):
                    prepare = True
            else:
                print("Error: Could not create pod mapping.", flush=True)
        elif nodes_topology == 0:
            print("Error: Topology file contains no nodes.", flush=True)
            sys.exit(1)
        else:
            print(
                f"Error: Deployment number of nodes ({nodes_dplymt_count}) and topology nodes ({nodes_topology}) must be equal.",
                flush=True)
            sys.exit(1)

    if prepare:
        print("Platform is now ready for testing..!", flush=True)
    else:
        print("Platform could not be ready due to errors.", flush=True)