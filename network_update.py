'''
This code is created on 30 July 2025.
It will update AC json file with weight average, total nodes and total edges.
This because we want to compare BA and AC network edges and average weight.
'''


import json, os, argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Create network topology using networkx graphs and save it to json file")
    parser.add_argument('--filename', type=str, required=True, help="Original filename")
    args = parser.parse_args()

    # Construct the full file path
    # Assuming 'topology' is a subdirectory relative to where you run the script
    file_path = os.path.join("topology", args.filename)

    # --- CORRECT WAY TO LOAD JSON FROM A FILE ---
    try:
        with open(file_path, 'r') as f:
            data = json.load(f) # Use json.load() to read from a file object
        print(f"Successfully loaded data from '{file_path}'")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        exit(1) # Exit the script if the file isn't found
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{file_path}'. Please check if it's valid JSON.")
        exit(1) # Exit the script if JSON is invalid
    # --- END OF CORRECTION ---

    # Calculate total_edges
    total_edges = len(data.get("edges", []))

    # Calculate total_nodes
    total_nodes = len(data.get("nodes"))

    # Calculate weight_average
    edges = data.get("edges", [])
    if edges:
        total_weight = sum(edge.get("weight", 0) for edge in edges)
        weight_average = total_weight / total_edges
    else:
        weight_average = 0.0 # Handle case with no edges to avoid division by zero

    # Update the 'graph' dictionary with the new fields
    # if "graph" not in data:
    #     data["graph"] = {}
    data["total_edges"] = total_edges
    data["total_nodes"] = total_nodes
    data["weight_average"] = f"{weight_average:.4f}"

    # Save the updated JSON data back to the file
    # You might want to save to a new file to keep the original,
    # or ensure you intend to overwrite. Here, it will overwrite the input file.
    with open(file_path, 'w') as f: # Use file_path for saving as well
        json.dump(data, f, indent=2)
    print(f"File '{file_path}' updated successfully.")