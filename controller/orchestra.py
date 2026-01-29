import os
import glob
import time
import subprocess
import uuid
import logging
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from google.cloud import bigquery

# ==========================================
# 🔧 USER CONFIGURATION (EDIT THIS BEFORE RUNNING)
# ==========================================
NODE_COUNT = 5           # <--- MANUAL SETTING: The size of the cluster for this batch
PROJECT_ID = "stoked-cosine-415611"
ZONE = "us-central1-c"
BUCKET_NAME = "pulun-phd-experiments"
TOPOLOGY_FOLDER = "topology"        # Folder containing your .json files
GMAIL_USER = "your.email@gmail.com" # Optional
GMAIL_PASS = "xxxx xxxx xxxx xxxx"  # Optional
# EXPERIMENT_DURATION = 600           # Duration in seconds (e.g., 10 mins)
EXPERIMENT_DURATION = 20
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
# 📂 1. THE SCANNER (SIMPLE)
# ==========================================
def get_experiment_files():
    """Just gets all .json files in the folder."""
    files = glob.glob(os.path.join(TOPOLOGY_FOLDER, "*.json"))
    files.sort() # Run in alphabetical order
    
    log(f"📂 Found {len(files)} topology files. Target Cluster Size: {NODE_COUNT}")
    return files

# ==========================================
# ☁️ 2. CLUSTER MANAGEMENT
# ==========================================
def create_cluster(node_count):
    # cluster_name = f"gossip-cluster-{node_count}"
    cluster_name = "bcgossip-cluster"
    
    log(f"🔨 CREATING {node_count}-node cluster (This takes ~5-10 mins)...")
    try:
        # Check if it exists first to avoid error? 
        # Or just run create (it will fail fast if exists, which is fine)
        run_cmd([
            "gcloud", "container", "clusters", "create", cluster_name,
            "--zone", ZONE,
            "--num-nodes", str(node_count),
            "--machine-type", "e2-medium", 
            "--disk-type", "pd-standard", "--disk-size", "20",
            "--quiet"
        ])
    except:
        log("⚠️ Cluster creation command returned error (maybe it already exists?). Proceeding...")

    # Always get credentials to be sure
    run_cmd([
        "gcloud", "container", "clusters", "get-credentials", cluster_name,
        "--zone", ZONE, "--project", PROJECT_ID
    ])
    return cluster_name

def delete_cluster(cluster_name):
    try:
        run_cmd([
            "gcloud", "container", "clusters", "delete", cluster_name,
            "--zone", ZONE, "--quiet"
        ])
        log("✅ Cluster deleted.")
    except Exception as e:
        log(f"⚠️ Cluster delete failed: {e}")

# ==========================================
# 💾 3. DATA EXPORT
# ==========================================
def export_data(test_id):
    log(f"📉 Exporting logs for ID: {test_id}...")
    try:
        bq = bigquery.Client(project=PROJECT_ID)
        
        query = f"""
            EXPORT DATA OPTIONS(
              uri='gs://{BUCKET_NAME}/{test_id}/logs_*.csv',
              format='CSV', overwrite=true, header=true
            ) AS
            SELECT * FROM `{PROJECT_ID}.gossip_dataset.gossip_logs`
            WHERE jsonPayload.test_id = '{test_id}'
        """
        # Uncomment to enable:
        # bq.query(query).result()
        log("✅ Data export triggered.")
    except Exception as e:
        log(f"⚠️ Export failed: {e}")

# ==========================================
# 🚀 4. MAIN LOOP
# ==========================================
def main():
    files = get_experiment_files()
    if not files: 
        log("❌ No files found. Exiting.")
        return

    # 1. SETUP INFRASTRUCTURE (Once for the whole batch)
    cluster_name = create_cluster(NODE_COUNT)
    
    # 2. INSTALL HELM (Once)
    log("Deploying Helm Chart...")
    # run_cmd("helm install my-gossip ./gossip-chart", shell=True)
    time.sleep(60) # Wait for pods to come up

    try:
        # 3. LOOP THROUGH FILES
        for i, filepath in enumerate(files):
            filename = os.path.basename(filepath)
            
            # Generate Unique ID
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            short_uuid = uuid.uuid4().hex[:4]
            TEST_ID = f"{filename.replace('.json', '')}_{timestamp}_{short_uuid}"

            log(f"\n[{i+1}/{len(files)}] 🚀 STARTING EXPERIMENT: {filename}")
            log(f"🆔 TEST ID: {TEST_ID}")

            try:
                # A. PREPARE (Inject Topology)
                log(f"Injecting Topology: {filename}")
                # run_cmd(f"python3 prepare.py --filename {filename}", shell=True)

                # B. EXECUTE (Gossip Trigger)
                log("Triggering Gossip...")
                # run_cmd(f"python3 automate.py --trigger --test-id {TEST_ID}", shell=True)

                # C. WAIT
                log(f"Running experiment for {EXPERIMENT_DURATION}s...")
                # time.sleep(EXPERIMENT_DURATION)
                time.sleep(EXPERIMENT_DURATION)

                                
                # D. FLUSH
                log("Waiting 60s for log flush...")
                # time.sleep(60)
                time.sleep(10)

                # E. EXPORT
                # export_data(TEST_ID)
                
                # send_email(f"DONE: {filename}", f"Test ID: {TEST_ID}")

            except Exception as e:
                log(f"❌ FAILED: {filename} - {e}")
                # send_email(f"FAILED: {filename}", str(e))
                # Continue to next file even if this one fails

    finally:
        # 4. TEARDOWN (Always happens, even if script crashes)
        log("🏁 All jobs finished. Deleting Cluster...")
        delete_cluster(cluster_name)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\n🛑 Script stopped by user. PLEASE CHECK GKE CONSOLE TO AVOID BILLING!")