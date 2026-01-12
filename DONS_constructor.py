# dons_network_converter.py

import json
import os
import sys
import argparse
import heapq
import time

def load_topology_from_json(json_file_path: str, topology_dir="topology") -> dict:
    """
    Loads a network topology from a JSON file and prepares it for processing.

    Args:
        json_file_path (str): Path to the input JSON file.

    Returns:
        dict: A dictionary containing the number of nodes and the adjacency list.
    """
    
    # Get current file and path
    json_file_path = os.path.join(topology_dir, json_file_path)
    
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{json_file_path}' was not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{json_file_path}'. Please check file format.")
        sys.exit(1)

    nodes = data.get('nodes', [])
    edges = data.get('edges', [])
    num_nodes = len(nodes)
    adj_list = {i: {} for i in range(num_nodes)}
    id_to_index = {node['id']: i for i, node in enumerate(nodes)}
    index_to_id = {i: node['id'] for i, node in enumerate(nodes)}

    for edge in edges:
        source_id = edge['source']
        target_id = edge['target']
        weight = edge.get('weight', 1)  # Default weight to 1 if not specified
        
        source_index = id_to_index.get(source_id)
        target_index = id_to_index.get(target_id)
        
        if source_index is not None and target_index is not None:
            adj_list[source_index][target_index] = weight
            adj_list[target_index][source_index] = weight

    if num_nodes == 0:
        print("Error: The input JSON file contains no nodes.")
        sys.exit(1)
        
    return {'num_nodes': num_nodes, 'adj_list': adj_list, 'original_data': data, 'index_to_id': id_to_index}


def prims_mst_edges(num_nodes: int, adj_list: dict) -> list:
    """
    Computes the edges of the Minimum Spanning Tree (MST) using Prim's algorithm.
    This simulates the DONS leader's role in finding the most efficient path.

    Args:
        num_nodes (int): The total number of nodes in the graph.
        adj_list (dict): The adjacency list of the graph.

    Returns:
        list: A list of edges representing the MST (source, target, weight).
    """
    if num_nodes == 0:
        return []

    mst = []
    # Using a set of node indices for visited tracking
    visited_indices = {0} # Start from node index 0
    min_heap = []

    # Initialize the heap with edges from the starting node (index 0)
    for neighbor, weight in adj_list.get(0, {}).items():
        heapq.heappush(min_heap, (weight, 0, neighbor))

    while min_heap and len(visited_indices) < num_nodes:
        weight, u_idx, v_idx = heapq.heappop(min_heap)

        if v_idx in visited_indices:
            continue

        visited_indices.add(v_idx)
        mst.append((u_idx, v_idx, weight))

        for neighbor, edge_weight in adj_list.get(v_idx, {}).items():
            if neighbor not in visited_indices:
                heapq.heappush(min_heap, (edge_weight, v_idx, neighbor))

    if len(mst) != num_nodes - 1 and num_nodes > 1:
        print("Warning: The graph is not fully connected, MST is incomplete.")
        
        
    # print(f"mst={mst}")
    return mst

def find_ons(num_nodes: int, mst_edges: list) -> dict:
    """
    Finds the Optimized Neighbor Set (ONS) for each node from the MST.
    This simulates each node processing the broadcasted MST.

    Args:
        num_nodes (int): The total number of nodes.
        mst_edges (list): The edges of the MST.

    Returns:
        dict: A dictionary where keys are node IDs and values are their ONS (neighbor: weight).
    """
    ons_sets = {i: {} for i in range(num_nodes)}
    for u, v, weight in mst_edges:
        ons_sets[u][v] = weight
        ons_sets[v][u] = weight
    return ons_sets

def create_output_json(file_path: str, original_data: dict, ons_sets: dict, ons_computation_time_ms: float, mst_edges: list, topology_dir="topology"):
    """
    Saves the original topology data along with the calculated ONS to a new JSON file.

    Args:
        file_path (str): The path to save the output JSON file.
        original_data (dict): The data loaded from the input JSON file.
        ons_sets (dict): The calculated ONS for each node.
        ons_computation_time_ms (float): Time taken to compute the ONS.
    """
    # Create a new dictionary to hold the combined data
    output_data = original_data.copy()

    # Convert MST edges from indices to original node IDs
    index_to_id = {i: node['id'] for i, node in enumerate(output_data['nodes'])}
    mst_edges_with_ids = []
    for u_idx, v_idx, weight in mst_edges:
        source_id = index_to_id[u_idx]
        target_id = index_to_id[v_idx]
        mst_edges_with_ids.append({"source": source_id, "target": target_id, "weight": weight})

    # Replace original edges with only the MST edges
    output_data['edges'] = mst_edges_with_ids

    # Convert ONS sets from indices to original node IDs
    ons_sets_with_ids = {}
    for node_idx, neighbors in ons_sets.items():
        node_id = index_to_id[node_idx]
        ons_sets_with_ids[node_id] = {index_to_id[neighbor_idx]: weight for neighbor_idx, weight in neighbors.items()}
    
    # Add the ONS data
    # output_data['ons_sets'] = ons_sets_with_ids
    
    # Add new metadata fields
    total_edges = len(output_data.get('edges', []))
    total_weight = sum(edge.get('weight', 0) for edge in output_data.get('edges', []))
    weight_average = total_weight / total_edges if total_edges > 0 else 0

    output_data['ons_computation_time_ms'] = f"{ons_computation_time_ms:.4f}"
    output_data['total_nodes'] = len(output_data.get('nodes', []))
    output_data['total_edges'] = total_edges
    output_data['weight_average'] = f"{weight_average:.4f}"

    # Get current file and path
    json_file_path = os.path.join(topology_dir, file_path)
    try:
        with open(json_file_path, 'w') as f:
            json.dump(output_data, f, indent=4)
        print(f"Successfully created DONS-optimized network at: {json_file_path}")
    except Exception as e:
        print(f"Error writing to file {file_path}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert an existing BA or ER network to a DONS-optimized model.")
    parser.add_argument("--input_file", type=str, required=True, help="Path to the input JSON file (BA/ER network).")
    parser.add_argument("--output_file", type=str, help="Path for the output JSON file.")
    args = parser.parse_args()

    # Load the base topology from the input JSON file
    topology_data = load_topology_from_json(args.input_file)
    num_nodes = topology_data['num_nodes']
    adj_list = topology_data['adj_list']
    original_data = topology_data['original_data']
    index_to_id = topology_data['index_to_id']
    
    print(f"Loaded network from '{args.input_file}' with {num_nodes} nodes.")

    # Simulate the DONS leader's work
    start_time = time.perf_counter()
    mst_edges = prims_mst_edges(num_nodes, adj_list)
    nodes_ons = find_ons(num_nodes, mst_edges)
    end_time = time.perf_counter()
    ons_computation_time_ms = (end_time - start_time) * 1000

    print("MST and ONS computation complete.")

    # Determine the output file path
    if args.output_file:
        output_file_path = args.output_file
    else:
        # Replicates AC file naming convention
        dir_name = os.path.dirname(args.input_file)
        file_name, file_extension = os.path.splitext(os.path.basename(args.input_file))
        output_file_path = os.path.join(dir_name, f"{file_name}_ons{file_extension}")
        print(f"No output file specified. Defaulting to: {output_file_path}")
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_file_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Create the final output JSON file
    create_output_json(output_file_path, original_data, nodes_ons, ons_computation_time_ms, mst_edges)
    
    print("DONS optimization complete.")
