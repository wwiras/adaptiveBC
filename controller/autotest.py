import subprocess
import time
import sys
import logging

# --- CONFIGURATION ---
PROJECT_ID = "stoked-cosine-415611"
ZONE = "us-central1-a"
NODE_COUNT = 5
CLUSTER_NAME = f"test-cluster-{NODE_COUNT}"

# --- SETUP LOGGING (Screen + File) ---
# This allows you to "cat experiment.log" to check status without opening tmux
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler("experiment.log"), # Save to file
        logging.StreamHandler(sys.stdout)      # Print to screen
    ]
)

def log(msg):
    logging.info(msg)

def run_cmd(command):
    try:
        log(f"executing: {' '.join(command)}")
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        log(f"❌ ERROR: {e}")
        raise

try:
    log("🚀 STARTING SANITY CHECK " + str(NODE_COUNT) + " Nodes)")

    # 1. Create Cluster
    log("Creating Cluster (This will take ~5-8 mins)...")
    run_cmd([
        "gcloud", "container", "clusters", "create", CLUSTER_NAME,
        "--zone", ZONE,
        "--num-nodes", str(NODE_COUNT),
        "--machine-type", "e2-medium",
        "--disk-size", "20",
        "--quiet"
    ])

    # 2. Get Credentials
    log("Fetching credentials...")
    run_cmd([
        "gcloud", "container", "clusters", "get-credentials", CLUSTER_NAME,
        "--zone", ZONE,
        "--project", PROJECT_ID
    ])

    # 3. Verify Nodes (Using shell=True for kubectl output)
    log("Checking node status...")
    subprocess.run("kubectl get nodes", shell=True)

    # 4. Simulate Work
    log("✅ Cluster is UP! Sleeping for 15 seconds...")
    time.sleep(15)

except Exception as e:
    log(f"⚠️ SCRIPT FAILED: {e}")

finally:
    log("🧹 CLEANUP: Deleting cluster...")
    try:
        run_cmd([
            "gcloud", "container", "clusters", "delete", CLUSTER_NAME,
            "--zone", ZONE,
            "--quiet"
        ])
        log("✅ DONE. Cluster deleted.")
    except Exception as e:
        log(f"❌ CRITICAL: Delete failed. PLEASE DELETE MANUALLY: {e}")
