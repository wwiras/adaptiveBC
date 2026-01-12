import json
import numpy as np
import argparse
import os
import random
import time


def load_graph_from_json(json_file_path):
    """
    Loads a graph from a JSON file and converts it into a weighted distance matrix.
    The JSON is expected to contain 'nodes' and 'edges' lists.
    The 'weight' from the edges is used as the distance.

    Args:
        json_file_path (str): The path to the JSON file.

    Returns:
        tuple: A tuple containing:
               - The number of nodes (int).
               - The distance matrix (np.array).
               - A dictionary mapping original node IDs to their index (int) in the matrix.
    """
    with open(json_file_path, 'r') as f:
        graph_data = json.load(f)

    node_ids = [node['id'] for node in graph_data['nodes']]
    num_nodes = len(node_ids)

    id_to_index = {node_id: i for i, node_id in enumerate(node_ids)}

    distance_matrix = np.full((num_nodes, num_nodes), float('inf'))
    np.fill_diagonal(distance_matrix, 0)

    for edge in graph_data['edges']:
        source_id = edge['source']
        target_id = edge['target']
        weight = edge['weight']

        source_index = id_to_index[source_id]
        target_index = id_to_index[target_id]

        distance_matrix[source_index, target_index] = weight
        distance_matrix[target_index, source_index] = weight

    return num_nodes, distance_matrix, id_to_index


def calculate_distance(cluster_a, cluster_b, distance_matrix):
    """
    Calculates the distance between two clusters using complete linkage.
    """
    current_max_distance = float('-inf')
    found_valid_connection = False

    for node_i in cluster_a:
        for node_j in cluster_b:
            if node_i == node_j:
                continue

            distance = distance_matrix[node_i][node_j]

            if distance == float('inf'):
                continue

            found_valid_connection = True
            if distance > current_max_distance:
                current_max_distance = distance

    if not found_valid_connection:
        return float('inf')
    else:
        return current_max_distance


def agglomerative_clustering(num_nodes, num_clusters, distance_matrix):
    """
    Performs agglomerative clustering.
    """
    clusters = [[i] for i in range(num_nodes)]

    while len(clusters) > num_clusters:
        min_distance = float('inf')
        clusters_to_merge = None

        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                distance = calculate_distance(clusters[i], clusters[j], distance_matrix)

                if distance < min_distance:
                    min_distance = distance
                    clusters_to_merge = (i, j)

        if clusters_to_merge is not None:
            index_a, index_b = clusters_to_merge

            if index_a > index_b:
                index_a, index_b = index_b, index_a

            new_cluster = clusters[index_a] + clusters[index_b]

            clusters.pop(index_b)
            clusters.pop(index_a)
            clusters.append(new_cluster)
        else:
            print("Warning: No more clusters could be merged. Remaining clusters might be disconnected.")
            break

    return clusters


def select_cluster_leaders(clusters, index_to_id_map):
    """
    Selects a leader for each cluster randomly.
    """
    cluster_leaders = {}
    for i, cluster in enumerate(clusters):
        leader_index = random.choice(cluster)
        leader_id = index_to_id_map[leader_index]
        cluster_leaders[f"Cluster {i + 1}"] = leader_id
    return cluster_leaders


def compute_mst_for_cluster(cluster_nodes, distance_matrix, start_node_index):
    """
    Constructs an MST for a single cluster using a Prim's-like approach.
    """
    local_index_map = {node: i for i, node in enumerate(cluster_nodes)}
    num_cluster_nodes = len(cluster_nodes)
    start_local_index = local_index_map[start_node_index]

    d = np.full(num_cluster_nodes, float('inf'))
    p = np.full(num_cluster_nodes, -1, dtype=int)

    d[start_local_index] = 0
    Q = set(range(num_cluster_nodes))

    while Q:
        u_local_index = min(Q, key=lambda i: d[i])
        u_global_index = cluster_nodes[u_local_index]

        if d[u_local_index] == float('inf'):
            print(f"  Warning: Sub-graph disconnected from start node. Cannot build full MST.")
            break

        Q.remove(u_local_index)

        for v_local_index in Q:
            v_global_index = cluster_nodes[v_local_index]
            weight = distance_matrix[u_global_index, v_global_index]

            if weight < d[v_local_index]:
                d[v_local_index] = weight
                p[v_local_index] = u_local_index

    mst_edges = []
    root_node_index = start_node_index

    for i in range(num_cluster_nodes):
        if i == start_local_index:
            continue

        child_global_index = cluster_nodes[i]
        parent_local_index = p[i]

        if parent_local_index != -1:
            parent_global_index = cluster_nodes[parent_local_index]
            edge_weight = distance_matrix[parent_global_index, child_global_index]
            mst_edges.append((parent_global_index, child_global_index, edge_weight))

    return mst_edges, root_node_index


def construct_comprehensive_mst(clusters, distance_matrix):
    """
    Constructs a comprehensive MST (MSTcom) for the entire network.
    """
    comprehensive_mst = []
    root_nodes = []
    cluster_mst_edges = []

    print("\n--- Phase 3: MST Construction (Parallel Logic) ---")

    for i, cluster in enumerate(clusters):
        start_node_index = cluster[0]

        mst_edges, root_node_index = compute_mst_for_cluster(cluster, distance_matrix, start_node_index)

        root_nodes.append(root_node_index)
        cluster_mst_edges.append(mst_edges)

        print(f"  Cluster {i + 1} MST Edges (Root: {root_node_index}):")
        for edge in mst_edges:
            print(f"    Edge: {edge[0]} -> {edge[1]}, Weight: {edge[2]}")

    print("\n--- Connecting MST Root Nodes ---")
    if len(root_nodes) > 1:
        root_connection_matrix = np.full((len(clusters), len(clusters)), float('inf'))
        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                cluster_a_nodes = clusters[i]
                cluster_b_nodes = clusters[j]

                min_weight = float('inf')
                for node_a in cluster_a_nodes:
                    for node_b in cluster_b_nodes:
                        weight = distance_matrix[node_a, node_b]
                        if weight < min_weight:
                            min_weight = weight

                if min_weight != float('inf'):
                    root_connection_matrix[i, j] = min_weight
                    root_connection_matrix[j, i] = min_weight

        pseudo_root_cluster = list(range(len(clusters)))
        start_pseudo_root_index = 0

        mst_root_edges, _ = compute_mst_for_cluster(pseudo_root_cluster, root_connection_matrix,
                                                    start_pseudo_root_index)

        print(f"  Inter-cluster connecting edges:")
        for edge in mst_root_edges:
            source_root_id = root_nodes[edge[0]]
            target_root_id = root_nodes[edge[1]]
            weight = edge[2]
            comprehensive_mst.append((source_root_id, target_root_id, weight))
            print(f"    Edge: {source_root_id} -> {target_root_id}, Weight: {weight}")
    else:
        print("  Only one cluster, no inter-cluster connections needed.")

    for mst_edges in cluster_mst_edges:
        comprehensive_mst.extend(mst_edges)

    return comprehensive_mst


def find_mon(comprehensive_mst, node_index):
    """
    Finds the MST Optimal Neighbors (MON) for a given node.
    This implements Algorithm 4.
    """
    mon_neighbors = {}

    for source, target, weight in comprehensive_mst:
        if source == node_index:
            mon_neighbors[target] = weight
        elif target == node_index:
            mon_neighbors[source] = weight

    return mon_neighbors


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Perform Agglomerative Clustering on a network graph from a JSON file.")
    parser.add_argument("--json_file_path", type=str, help="Path to the JSON file containing the graph topology.",
                        required=True)
    parser.add_argument("--num_clusters", type=int, help="Desired number of clusters (M).", required=True)
    args = parser.parse_args()

    json_file_path = os.path.join("topology", args.json_file_path)
    print(f"Loading network from : {json_file_path}")
    M = args.num_clusters
    print(f"Total cluster(s), M : {M}")

    if not os.path.exists(json_file_path):
        print(f"Error: JSON file not found at '{json_file_path}'")
        exit(1)

    if M <= 0:
        print("Error: The desired number of clusters (M) must be a positive integer.")
        exit(1)

    try:
        num_nodes, distance_matrix, id_to_index = load_graph_from_json(json_file_path)

        if M > num_nodes:
            print(
                f"Warning: Desired number of clusters (M={M}) is greater than the total number of nodes ({num_nodes}).")
            M = num_nodes

        print("\n--- Loaded Distance Matrix ---")
        np.set_printoptions(linewidth=np.inf, precision=2, suppress=True,
                            formatter={'float_kind': lambda x: "inf" if np.isinf(x) else f"{x:.0f}"})
        print(distance_matrix)
        print("\n")

        # Phase 1: Agglomerative Clustering
        start_time = time.perf_counter()
        clustered_result = agglomerative_clustering(num_nodes, M, distance_matrix)
        end_time = time.perf_counter()

        # Convert time to milliseconds
        clustering_time_ms = (end_time - start_time) * 1000

        index_to_id = {v: k for k, v in id_to_index.items()}
        final_clusters_with_ids = [[index_to_id[node_index] for node_index in cluster] for cluster in clustered_result]

        print(f"--- BNSF Process Completed ---")
        print(f"\nTime taken to produce AC clusters: {clustering_time_ms:.4f} milliseconds")
        print(f"\n--- Final Clusters (M = {M}) ---")
        for i, cluster in enumerate(final_clusters_with_ids):
            print(f"Cluster {i + 1}: {cluster}")

        # Phase 2: Leader Selection and Announcement
        cluster_leaders = select_cluster_leaders(clustered_result, index_to_id)

        print(f"\n--- Leader Selection & Announcement ---")
        for cluster_name, leader_id in cluster_leaders.items():
            print(f"{cluster_name} Leader: {leader_id}")

        # Phase 3: MST Construction
        comprehensive_mst = construct_comprehensive_mst(clustered_result, distance_matrix)

        print(f"\n--- Comprehensive MST for the Entire Network ---")
        for i, edge in enumerate(comprehensive_mst):
            source_id = index_to_id[edge[0]]
            target_id = index_to_id[edge[1]]
            weight = edge[2]
            print(f"  Edge {i + 1}: {source_id} -> {target_id}, Weight: {weight}")

        # Phase 4: Neighbor Selection
        print(f"\n--- Phase 4: Neighbor Selection (MON) ---")
        for node_index in range(num_nodes):
            mon = find_mon(comprehensive_mst, node_index)
            node_id = index_to_id[node_index]

            print(f"  Node {node_id}'s Optimal Neighbors:")
            if mon:
                for neighbor_index, weight in mon.items():
                    neighbor_id = index_to_id[neighbor_index]
                    print(f"    - {neighbor_id} (Weight: {weight})")
            else:
                print(f"    - No optimal neighbors found in MST.")

        # Phase 5: Build new overlay topology JSON file
        print(f"\n--- Phase 5: Building New Overlay Topology File ---")

        base_name = os.path.basename(json_file_path)
        file_name, file_extension = os.path.splitext(base_name)
        new_file_name = f"{file_name}_AC{M}{file_extension}"
        output_dir = os.path.dirname(json_file_path)
        new_file_path = os.path.join(output_dir, new_file_name)

        nodes_list = [{"id": node_id} for node_id in id_to_index.keys()]
        edges_list = []
        for source_index, target_index, weight in comprehensive_mst:
            source_id = index_to_id[source_index]
            target_id = index_to_id[target_index]
            edges_list.append({"source": source_id, "target": target_id, "weight": weight})

        # Calculate weight_average
        if edges_list:
            total_weight = sum(edge.get("weight", 0) for edge in edges_list)
            weight_average = total_weight / len(edges_list)
        else:
            weight_average = 0.0  # Handle case with no edges to avoid division by zero

        new_topology = {
            "directed": False,
            "multigraph": False,
            "graph": {},
            "nodes": nodes_list,
            "edges": edges_list,
            "total_clusters": M,
            "clustering_time_ms": f"{clustering_time_ms:.4f}",
            "total_nodes": len(nodes_list),
            "total_edges": len(edges_list),
            "weight_average": f"{weight_average:.4f}"
        }

        with open(new_file_path, 'w') as f:
            json.dump(new_topology, f, indent=2)

        print(f"Successfully created new overlay topology file at: {new_file_path}")
        print(f"Total clusters (M) recorded in file: {M}")
        print(f"Clustering time recorded in file: {clustering_time_ms:.4f} milliseconds")

    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{json_file_path}'. Please ensure it's a valid JSON file.")
        exit(1)
    except KeyError as e:
        print(f"Error: Missing expected key in JSON file: {e}. Please check the JSON structure.")
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        exit(1)