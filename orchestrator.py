import os
import glob
import time
import subprocess
import uuid
import logging
import smtplib
import re
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
K8SNODE_COUNT = 5  

# Workload Settings (Helm/Pods)
IMAGE_NAME = "wwiras/simcl2"
IMAGE_TAG = "v14"
BUCKET_NAME = "pulun-phd-experiments"
TOPOLOGY_FOLDER = "topology"
EXPERIMENT_DURATION = 100
HELM_CHART_FOLDER = "simcl2" # <--- Name of the folder containing 'chartsim'

# Email (Optional)
GMAIL_USER = "your.email@gmail.com"
GMAIL_PASS = "xxxx xxxx xxxx xxxx"

# ==========================================
# 📝 LOGGING & UTILS
# ==========================================
# Generate a timestamped log filename
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"orchestrator_{timestamp_str}.log"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(log_filename), # <--- Logs to timestamped file
        logging.StreamHandler()            # <--- Logs to terminal
    ]
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
        run_cmd([
            "gcloud", "container", "clusters", "create", K8SCLUSTER_NAME,
            "--zone", ZONE, "--num-nodes", str(node_count),
            "--machine-type", "e2-medium", "--quiet"
        ])
    except:
        log("⚠️ Cluster creation skipped (likely already exists). Proceeding...")
    
    run_cmd([
        "gcloud", "container", "clusters", "get-credentials", K8SCLUSTER_NAME,
        "--zone", ZONE, "--project", PROJECT_ID
    ])

def delete_cluster():
    try:
        run_cmd(["gcloud", "container", "clusters", "delete", K8SCLUSTER_NAME, "--zone", ZONE, "--quiet"])
        log("✅ Cluster deleted.")
    except Exception as e:
        log(f"⚠️ Cluster delete failed: {e}")

# ==========================================
# 🚀 3. MAIN LOOP
# ==========================================
def main():
    log(f"🎬 Script started. Logs will be saved to: {log_filename}")
    
    # Store the root directory so we can always come back
    ROOT_DIR = os.getcwd() 
    
    files = get_experiment_files()
    if not files: 
        log("❌ No files found. Exiting.")
        return

    # 1. SETUP INFRASTRUCTURE
    create_cluster(K8SNODE_COUNT)
    
    try:
        # 2. LOOP THROUGH FILES
        for i, filepath in enumerate(files):
            filename = os.path.basename(filepath)
            
            # --- A. EXTRACT VARIABLES ---
            p2p_nodes = 0
            # Regex logic...
            node_match = re.search(r"nodes(\d+)", filename)
            if node_match: p2p_nodes = int(node_match.group(1))

            TEST_ID = f"{filename.replace('.json', '')}_{datetime.now().strftime('%Y%m%d_%H%M')}"

            log(f"\n[{i+1}/{len(files)}] 🚀 STARTING EXPERIMENT: {filename}")
            log(f"   🔹 P2P Nodes: {p2p_nodes} | ID: {TEST_ID}")

            try:
                # --- B. DEPLOY WORKLOAD (HELM) ---
                log(f"📂 Changing directory to '{HELM_CHART_FOLDER}' for Helm...")
                os.chdir(HELM_CHART_FOLDER) # <--- GO INTO FOLDER
                
                try:
                    helm_cmd = (
                        f"helm install simcn ./chartsim "
                        f"--set testType=default,totalNodes={p2p_nodes},"
                        f"image.tag={IMAGE_TAG},image.name={IMAGE_NAME} "
                        f"--debug"
                    )
                    run_cmd(helm_cmd, shell=True)
                finally:
                    # ALWAYS go back to root, even if helm fails
                    os.chdir(ROOT_DIR) # <--- GO BACK
                    log(f"📂 Returned to root directory: {ROOT_DIR}")

                # Wait for pods
                log("Waiting 60s for pods to initialize...")
                time.sleep(60)

                # --- C. PREPARE ---
                log(f"Injecting Topology: {filename}")
                # run_cmd(f"python3 prepare.py --filename {filename}", shell=True)

                # --- D. EXECUTE ---
                log("Triggering Gossip...")
                # run_cmd(f"python3 automate.py --trigger --test-id {TEST_ID}", shell=True)

                # --- E. WAIT ---
                log(f"Running experiment for {EXPERIMENT_DURATION}s...")
                time.sleep(EXPERIMENT_DURATION)
                
                # --- F. FLUSH ---
                log("Waiting 10s for log flush...")
                time.sleep(10)

            except Exception as e:
                log(f"❌ FAILED: {filename} - {e}")
                # Ensure we are back in root if crash happened inside the helm block
                if os.getcwd() != ROOT_DIR:
                    os.chdir(ROOT_DIR)
            
            finally:
                # --- CLEANUP WORKLOAD ---
                log("🧹 Cleaning up Helm release...")
                try:
                    run_cmd("helm uninstall simcn", shell=True)
                    time.sleep(30) 
                except:
                    log("⚠️ Helm uninstall failed.")

    finally:
        log("🏁 All jobs finished. Deleting Cluster...")
        delete_cluster()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\n🛑 Script stopped by user.")