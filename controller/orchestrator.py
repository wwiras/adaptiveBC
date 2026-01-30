import os
import glob
import time
import subprocess
import uuid
import logging
import smtplib
import re  # <--- Added for Regex
from datetime import datetime
from email.mime.text import MIMEText
from google.cloud import bigquery

# ==========================================
# 🔧 USER CONFIGURATION
# ==========================================
# Infrastructure Settings (GKE)
PROJECT_ID = "stoked-cosine-415611"
ZONE = "us-central1-c"
K8SCLUSTER_NAME = "bcgossip-cluster"
K8SNODE_COUNT = 5  # Physical nodes (ensure this is enough for your largest P2P test)

# Workload Settings (Helm/Pods)
IMAGE_NAME = "wwiras/simcl2"
IMAGE_TAG = "v14"
BUCKET_NAME = "pulun-phd-experiments"
TOPOLOGY_FOLDER = "topology"
EXPERIMENT_DURATION = 20

# Email (Optional)
GMAIL_USER = "your.email@gmail.com"
GMAIL_PASS = "xxxx xxxx xxxx xxxx"

# ==========================================
# 📝 LOGGING & UTILS
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler("orchestrator.log"), logging.StreamHandler()]
)

def log(msg):
    logging.info(msg)

def run_cmd(command, shell=False):
    """Runs a shell command and logs it."""
    cmd_str = command if shell else " ".join(command)
    try:
        subprocess.run(command, check=True, shell=shell)
    except subprocess.CalledProcessError as e:
        log(f"❌ COMMAND FAILED: {cmd_str}")
        raise e

def send_email(subject, body):
    try:
        if not GMAIL_PASS or "xxxx" in GMAIL_PASS: return
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = GMAIL_USER
        msg['To'] = GMAIL_USER
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(GMAIL_USER, GMAIL_PASS)
            server.sendmail(GMAIL_USER, GMAIL_USER, msg.as_string())
    except Exception:
        pass 

# ==========================================
# 📂 1. THE SCANNER
# ==========================================
def get_experiment_files():
    """Gets all .json files in the folder."""
    files = glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json"))
    files.sort()
    log(f"📂 Found {len(files)} topology files.")
    return files

# ==========================================
# ☁️ 2. CLUSTER MANAGEMENT
# ==========================================
def create_cluster(node_count):
    log(f"🔨 ENSURING CLUSTER {K8SCLUSTER_NAME} EXISTS ({node_count} nodes)...")
    try:
        # Create command
        run_cmd([
            "gcloud", "container", "clusters", "create", K8SCLUSTER_NAME,
            "--zone", ZONE,
            "--num-nodes", str(node_count),
            "--machine-type", "e2-medium", 
            "--disk-type", "pd-standard", "--disk-size", "20",
            "--quiet"
        ])
    except:
        log("⚠️ Cluster creation skipped (likely already exists). Proceeding...")

    # Get Credentials
    run_cmd([
        "gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME,
        "--zone", ZONE, "--project", PROJECT_ID
    ])

def delete_cluster():
    try:
        run_cmd([
            "gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME,
            "--zone", ZONE, "--quiet"
        ])
        log("✅ Cluster deleted.")
    except Exception as e:
        log(f"⚠️ Cluster delete failed: {e}")

# ==========================================
# 🚀 3. MAIN LOOP
# ==========================================
def main():
    files = get_experiment_files()
    if not files: 
        log("❌ No files found. Exiting.")
        return

    # 1. SETUP INFRASTRUCTURE (Run once at start)
    create_cluster(K8SNODE_COUNT)
    
    try:
        # 2. LOOP THROUGH FILES
        for i, filepath in enumerate(files):
            filename = os.path.basename(filepath)
            
            # --- A. EXTRACT VARIABLES FROM FILENAME ---
            # Default values
            p2p_nodes = 0
            clusters = 0
            degree = 0

            # Regex 1: Total Nodes (e.g., "nodes1000")
            node_match = re.search(r"nodes(\d+)", filename)
            if node_match: p2p_nodes = int(node_match.group(1))

            # Regex 2: Clusters (e.g., "_k8" or "_AC8")
            cluster_match = re.search(r"(_k|_AC)(\d+)", filename)
            if cluster_match: clusters = int(cluster_match.group(2))

            # Regex 3: Degree (e.g., "_BA5")
            degree_match = re.search(r"_BA(\d+)", filename)
            if degree_match: degree = int(degree_match.group(1))

            # --- GENERATE TEST ID ---
            # timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            short_uuid = uuid.uuid4().hex[:4]
            TEST_ID = f"{filename.replace('.json', '')}_{short_uuid}"

            log(f"\n[{i+1}/{len(files)}] 🚀 STARTING EXPERIMENT: {filename}")
            log(f"   🔹 P2P Nodes: {p2p_nodes} | Clusters: {clusters} | Degree: {degree}")
            log(f"   🆔 TEST ID: {TEST_ID}")

            try:
                # --- B. DEPLOY WORKLOAD (HELM) ---
                # We do this INSIDE the loop so 'totalNodes' updates per file
                log(f"Deploying Helm Chart with {p2p_nodes} pods...")
                helm_cmd = (
                    f"helm install simcn ./chartsim "
                    f"--set testType=default,totalNodes={p2p_nodes},"
                    f"image.tag={IMAGE_TAG},image.name={IMAGE_NAME} "
                    f"--debug"
                )
                run_cmd(helm_cmd, shell=True)
                
                # Wait for pods to start
                log("Waiting 60s for pods to initialize...")
                time.sleep(60)

                # --- C. PREPARE (Inject Topology) ---
                log(f"Injecting Topology: {filename}")
                # run_cmd(f"python3 prepare.py --filename {filename}", shell=True)

                # --- D. EXECUTE (Trigger Gossip) ---
                log("Triggering Gossip...")
                # run_cmd(f"python3 automate.py --trigger --test-id {TEST_ID}", shell=True)

                # --- E. WAIT (Experiment Running) ---
                log(f"Running experiment for {EXPERIMENT_DURATION}s...")
                time.sleep(EXPERIMENT_DURATION)
                                
                # --- F. FLUSH LOGS ---
                log("Waiting 10s for log flush...")
                time.sleep(10)

                # --- G. EXPORT DATA ---
                # export_data(TEST_ID)
                # send_email(f"DONE: {filename}", f"Test ID: {TEST_ID}")

            except Exception as e:
                log(f"❌ FAILED: {filename} - {e}")
            
            finally:
                # --- CLEANUP WORKLOAD (Crucial for next loop) ---
                log("🧹 Cleaning up Helm release...")
                try:
                    run_cmd("helm uninstall simcn", shell=True)
                    # Wait for pods to actually vanish before next install
                    time.sleep(30) 
                except:
                    log("⚠️ Helm uninstall failed (maybe it wasn't installed).")

    finally:
        # 3. TEARDOWN INFRASTRUCTURE (End of script)
        log("🏁 All jobs finished. Deleting Cluster...")
        delete_cluster()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\n🛑 Script stopped by user. PLEASE CHECK GKE CONSOLE TO AVOID BILLING!")