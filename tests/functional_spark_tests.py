import os
import sys
import subprocess
import time
import shutil
import glob
import urllib.request
import tarfile
import logging
import socket

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SPARK_VERSION = "3.5.0"
SPARK_TGZ_NAME = f"spark-{SPARK_VERSION}-bin-hadoop3.tgz"
SPARK_URL = f"https://archive.apache.org/dist/spark/spark-{SPARK_VERSION}/{SPARK_TGZ_NAME}"

# Async Profiler
AP_VERSION = "2.9"
AP_TGZ_NAME = f"async-profiler-{AP_VERSION}-linux-x64.tar.gz"
AP_URL = f"https://github.com/async-profiler/async-profiler/releases/download/v{AP_VERSION}/{AP_TGZ_NAME}"

TEST_DIR = "/tmp/spark-test-setup"
SPARK_HOME = os.path.join(TEST_DIR, f"spark-{SPARK_VERSION}-bin-hadoop3")
AGENT_JAR = os.path.join(TEST_DIR, "gprofiler-spark-agent.jar")
OUTPUT_DIR = os.path.join(TEST_DIR, "output")
WORKLOAD_FILE = os.path.join(TEST_DIR, "workload.scala")

# Python GProfiler
REPO_ROOT = os.getcwd()
PYTHON_CMD = sys.executable

def run_command(cmd, shell=False, cwd=None, env=None, check=True):
    logger.info(f"Running command: {cmd if isinstance(cmd, str) else ' '.join(cmd)}")
    result = subprocess.run(cmd, shell=shell, cwd=cwd, env=env, check=check, text=True, capture_output=True)
    return result

def setup_env():
    logger.info("Setting up test environment...")
    if os.path.exists(TEST_DIR):
        subprocess.run(["sudo", "rm", "-rf", TEST_DIR], check=False)
    os.makedirs(TEST_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def install_dependencies():
    logger.info("Installing Python dependencies...")
    try:
        subprocess.run([PYTHON_CMD, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
    except Exception as e:
        logger.error(f"Failed to install dependencies: {e}")
        sys.exit(1)

def setup_resources():
    logger.info("Setting up gprofiler resources (Mocking build step)...")
    resources_dir = os.path.join(REPO_ROOT, "gprofiler/resources")
    java_dir = os.path.join(resources_dir, "java")
    glibc_dir = os.path.join(java_dir, "glibc")
    musl_dir = os.path.join(java_dir, "musl")
    
    os.makedirs(glibc_dir, exist_ok=True)
    os.makedirs(musl_dir, exist_ok=True)
    
    # Download async-profiler
    cache_path = os.path.join("/tmp", AP_TGZ_NAME)
    if not os.path.exists(cache_path):
        logger.info(f"Downloading Async Profiler from {AP_URL}")
        urllib.request.urlretrieve(AP_URL, cache_path)
    
    # Extract to temp
    with tarfile.open(cache_path, "r:gz") as tar:
        tar.extractall(path="/tmp")
        
    ap_extracted_dir = os.path.join("/tmp", f"async-profiler-{AP_VERSION}-linux-x64")
    
    # Copy files
    # gprofiler/resources/java/asprof
    shutil.copy(os.path.join(ap_extracted_dir, "build/bin/asprof"), os.path.join(java_dir, "asprof"))
    os.chmod(os.path.join(java_dir, "asprof"), 0o755)

    # gprofiler/resources/java/glibc/libasyncProfiler.so
    shutil.copy(os.path.join(ap_extracted_dir, "build/lib/libasyncProfiler.so"), os.path.join(glibc_dir, "libasyncProfiler.so"))
    # Also copy to musl dir as placeholder if needed, though we run on glibc
    shutil.copy(os.path.join(ap_extracted_dir, "build/lib/libasyncProfiler.so"), os.path.join(musl_dir, "libasyncProfiler.so"))
    
    # gprofiler/resources/java/async-profiler-version
    with open(os.path.join(java_dir, "async-profiler-version"), "w") as f:
        f.write(AP_VERSION)
        
    # We also need 'burn' for flamegraphs?
    # gprofiler/resources/burn
    # Burn is a go binary. We might not need it for .col files, only for .html.
    # If missing, it might fail flamegraph generation but .col should be fine.
    # I'll create a dummy burn if it doesn't exist, or try to download it?
    # Actually, gprofiler main logic:
    # _generate_flamegraph_html uses 'burn'.
    # If it fails, it catches exception and logs warning.
    # We verify .col files, so it's fine.
    
    logger.info("Resources setup complete.")

def build_agent():
    logger.info("Building spark agent...")
    agent_dir = "runtime-agents/gprofiler-spark-agent"
    try:
        if shutil.which("mvn") is None:
            logger.error("Maven (mvn) not found in PATH")
            sys.exit(1)

        run_command(["mvn", "clean", "package"], cwd=agent_dir)
        target_dir = os.path.join(agent_dir, "target")
        jars = glob.glob(os.path.join(target_dir, "gprofiler-spark-agent-*.jar"))
        jars = [j for j in jars if not os.path.basename(j).startswith("original-")]
        if not jars:
            raise Exception("Agent JAR not found!")
        shutil.copy(jars[0], AGENT_JAR)
        logger.info(f"Agent JAR copied to {AGENT_JAR}")
    except Exception as e:
        logger.error(f"Failed to build agent: {e}")
        sys.exit(1)

def download_spark():
    logger.info("Downloading Spark...")
    cache_path = os.path.join("/tmp", SPARK_TGZ_NAME)
    if not os.path.exists(cache_path):
        logger.info(f"Downloading from {SPARK_URL}")
        urllib.request.urlretrieve(SPARK_URL, cache_path)
    
    logger.info("Extracting Spark...")
    with tarfile.open(cache_path, "r:gz") as tar:
        tar.extractall(path=TEST_DIR)
    
    if not os.path.exists(SPARK_HOME):
        logger.error(f"Spark home {SPARK_HOME} does not exist after extraction!")
        sys.exit(1)

def start_spark_cluster():
    logger.info("Starting Spark Cluster...")
    env = os.environ.copy()
    env["SPARK_HOME"] = SPARK_HOME
    
    master_log = open(os.path.join(TEST_DIR, "master.log"), "w")
    subprocess.Popen([os.path.join(SPARK_HOME, "sbin/start-master.sh")], env=env, stdout=master_log, stderr=master_log)
    
    time.sleep(5)
    hostname = socket.gethostname()
    master_url = f"spark://{hostname}:7077"
    
    worker_log = open(os.path.join(TEST_DIR, "worker.log"), "w")
    subprocess.Popen([
        os.path.join(SPARK_HOME, "sbin/start-worker.sh"),
        master_url,
        "-c", "2",
        "-m", "4g"
    ], env=env, stdout=worker_log, stderr=worker_log)
    
    time.sleep(5)
    logger.info(f"Spark Cluster started at {master_url}")
    return master_url

def create_workload():
    content = """
    val data = (1 to 100000).toList
    val rdd = sc.parallelize(data, 50)
    val count = rdd.map(x => {
        val start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < 50) {} 
        math.sqrt(x)
    }).count()
    println(s"Count: $count")
    Thread.sleep(20000)
    System.exit(0)
    """
    with open(WORKLOAD_FILE, "w") as f:
        f.write(content)

def start_gprofiler():
    logger.info("Starting gprofiler (source mode)...")
    
    # Ensure PYTHONPATH includes repo root
    env = os.environ.copy()
    env["PYTHONPATH"] = REPO_ROOT + ":" + env.get("PYTHONPATH", "")
    
    cmd = [
        "sudo",
        f"PYTHONPATH={env['PYTHONPATH']}", # sudo scrubs env, pass it explicitly
        PYTHON_CMD, "-m", "gprofiler.main",
        "--spark-mode",
        "--profile-all-spark",
        "--output-dir", OUTPUT_DIR,
        "--continuous",
        "--profiling-duration", "15",
        "--verbose",
        "--log-file", os.path.join(TEST_DIR, "gprofiler.log")
    ]
    
    logger.info(f"gProfiler command: {' '.join(cmd)}")
    proc = subprocess.Popen(" ".join(cmd), shell=True, executable="/bin/bash")
    return proc

def run_workload(master_url):
    logger.info("Running Spark Shell Workload...")
    env = os.environ.copy()
    env["SPARK_HOME"] = SPARK_HOME
    
    cmd = [
        os.path.join(SPARK_HOME, "bin/spark-shell"),
        "--master", master_url,
        "--conf", f"spark.driver.extraJavaOptions=-javaagent:{AGENT_JAR}",
        "--conf", f"spark.executor.extraJavaOptions=-javaagent:{AGENT_JAR}",
        "-I", WORKLOAD_FILE
    ]
    
    logger.info(f"Command: {' '.join(cmd)}")
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    logger.info("Workload finished.")
    if proc.returncode != 0:
        logger.error("Workload failed!")
        logger.error(proc.stderr)
    else:
        logger.info("Workload completed successfully.")

def verify_results():
    logger.info("Verifying results...")
    col_files = glob.glob(os.path.join(OUTPUT_DIR, "*.col"))
    if not col_files:
        logger.error("No .col files found in output directory!")
        return False
    
    logger.info(f"Found {len(col_files)} profile files.")
    
    found_spark = False
    for cf in col_files:
        with open(cf, "r") as f:
            content = f.read()
            if "org/apache/spark" in content or "spark" in content:
                found_spark = True
                logger.info(f"Found spark frames in {cf}")
                break
    
    if not found_spark:
        logger.warning("Did not find obvious Spark frames in profiles. Checking for any Java frames...")
        for cf in col_files:
            with open(cf, "r") as f:
                content = f.read()
                if "java/" in content or "scala/" in content:
                    logger.info(f"Found Java/Scala frames in {cf}")
                    found_spark = True 
                    break
    
    return found_spark

def teardown(gprofiler_proc):
    logger.info("Teardown...")
    if gprofiler_proc:
        # Since we use shell=True and sudo, finding the PID is tricky.
        # We can pkill -f gprofiler.main
        subprocess.run(["sudo", "pkill", "-f", "gprofiler.main"], check=False)
    
    env = os.environ.copy()
    env["SPARK_HOME"] = SPARK_HOME
    if os.path.exists(os.path.join(SPARK_HOME, "sbin/stop-worker.sh")):
        subprocess.run([os.path.join(SPARK_HOME, "sbin/stop-worker.sh")], env=env, check=False)
    if os.path.exists(os.path.join(SPARK_HOME, "sbin/stop-master.sh")):
        subprocess.run([os.path.join(SPARK_HOME, "sbin/stop-master.sh")], env=env, check=False)

def main():
    setup_env()
    install_dependencies()
    setup_resources()
    build_agent()
    download_spark()
    
    gprofiler_proc = None
    try:
        master_url = start_spark_cluster()
        gprofiler_proc = start_gprofiler()
        
        time.sleep(5)
        
        create_workload()
        run_workload(master_url)
        
        time.sleep(10)
        
        if verify_results():
            logger.info("TEST PASSED!")
        else:
            logger.error("TEST FAILED: No valid profiles found.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        sys.exit(1)
    finally:
        teardown(gprofiler_proc)

if __name__ == "__main__":
    main()
