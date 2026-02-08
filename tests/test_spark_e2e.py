import os
import shutil
import subprocess
import time
import requests
import pytest
import sys
import glob
from pathlib import Path

# Constants
SPARK_VERSION = "3.5.0"
SPARK_HADOOP_VERSION = "3"
SPARK_DIR = f"spark-{SPARK_VERSION}-bin-hadoop{SPARK_HADOOP_VERSION}"
SPARK_TGZ = f"{SPARK_DIR}.tgz"
SPARK_URL = f"https://archive.apache.org/dist/spark/spark-{SPARK_VERSION}/{SPARK_TGZ}"

# Paths
REPO_ROOT = Path(__file__).parent.parent.resolve()
BUILD_DIR = REPO_ROOT / "build" / "x86_64"
EXECUTABLE = BUILD_DIR / "gprofiler"
OUTPUT_DIR = REPO_ROOT / "spark_profile_output"

@pytest.fixture(scope="module")
def setup_environment():
    """Builds gprofiler and downloads Spark."""
    # 1. Build gprofiler
    print("Building gprofiler executable...")
    # Using --fast to skip staticx if possible, but for e2e full build is safer.
    subprocess.run([str(REPO_ROOT / "scripts" / "build_x86_64_executable.sh"), "--fast"], check=True, cwd=REPO_ROOT)
    if not EXECUTABLE.exists():
        pytest.fail("gprofiler executable not found!")

    # 2. Download Spark
    if not os.path.exists(SPARK_DIR):
        print(f"Downloading Spark {SPARK_VERSION}...")
        subprocess.run(["curl", "-O", SPARK_URL], check=True)
        subprocess.run(["tar", "-xzf", SPARK_TGZ], check=True)

    # 3. Create output directory
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir()

    yield

    # Cleanup (optional, keeping output for inspection)
    # shutil.rmtree(OUTPUT_DIR)

@pytest.fixture(scope="module")
def spark_cluster(setup_environment):
    """Starts a local Spark Master and Worker."""
    env = os.environ.copy()
    env["SPARK_HOME"] = str(REPO_ROOT / SPARK_DIR)

    # Start Master
    master_log = open("spark_master.log", "w")
    master_proc = subprocess.Popen(
        [f"./{SPARK_DIR}/sbin/start-master.sh"],
        env=env, stdout=master_log, stderr=master_log
    )
    time.sleep(5) # Wait for master to start

    # Start Worker
    worker_log = open("spark_worker.log", "w")
    worker_proc = subprocess.Popen(
        [f"./{SPARK_DIR}/sbin/start-worker.sh", "spark://localhost:7077"],
        env=env, stdout=worker_log, stderr=worker_log
    )
    time.sleep(5)

    yield "spark://localhost:7077"

    # Stop Cluster
    subprocess.run([f"./{SPARK_DIR}/sbin/stop-worker.sh"], env=env)
    subprocess.run([f"./{SPARK_DIR}/sbin/stop-master.sh"], env=env)
    master_log.close()
    worker_log.close()

def test_spark_profiling(spark_cluster):
    """Runs a Java Spark job and profiles it with gprofiler."""

    # 1. Compile Java Spark App
    spark_home = REPO_ROOT / SPARK_DIR
    spark_jars = glob.glob(str(spark_home / "jars" / "*.jar"))
    classpath = ":".join(spark_jars)

    java_src = """
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import java.util.ArrayList;
import java.util.List;

public class GProfilerTestApp {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("GProfilerTestApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("Spark App Started. Driver PID: " + name);

        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            data.add(i);
        }

        for (int i = 0; i < 60; i++) {
            JavaRDD<Integer> distData = sc.parallelize(data);
            long count = distData.count();
            System.out.println("Iteration " + i + ": " + count);
            Thread.sleep(1000);
        }

        sc.stop();
    }
}
"""

    Path("GProfilerTestApp.java").write_text(java_src)

    print("Compiling Java Spark App...")
    # Using subprocess.run with shell=False and list of arguments
    # Note: classpath globbing might be tricky if not expanded, but python's glob handles it.
    subprocess.run(["javac", "-cp", classpath, "GProfilerTestApp.java"], check=True)
    subprocess.run(["jar", "cf", "GProfilerTestApp.jar", "GProfilerTestApp.class"], check=True)

    env = os.environ.copy()
    env["SPARK_HOME"] = str(spark_home)

    print("Submitting Spark Application...")
    # Using Popen for async execution
    app_proc = subprocess.Popen(
        [
            f"./{SPARK_DIR}/bin/spark-submit",
            "--class", "GProfilerTestApp",
            "--master", spark_cluster,
            "GProfilerTestApp.jar"
        ],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # 2. Start gprofiler in --spark-mode
    print("Starting gprofiler in --spark-mode...")
    gprofiler_log = open("gprofiler_test.log", "w")
    gprofiler_proc = subprocess.Popen(
        [
            "sudo", str(EXECUTABLE),
            "--spark-mode",
            "--output-dir", str(OUTPUT_DIR),
            "--log-level", "debug",
            "--profile-all-spark" # Skip server-side allowlist check
        ],
        stdout=gprofiler_log, stderr=gprofiler_log
    )

    # 3. Wait for some profiles to be generated
    # The spark app runs for ~60 seconds.
    # gprofiler should detect it and start profiling.

    start_time = time.time()
    found_profile = False

    while time.time() - start_time < 50:
        # Check output directory for .col files (collapsed stacks) or .json
        files = list(OUTPUT_DIR.glob("*"))
        if len(files) > 0:
            print(f"Found profile output: {files}")
            found_profile = True
            break
        time.sleep(2)

    # Cleanup
    app_proc.terminate()
    subprocess.run(["sudo", "pkill", "-f", "gprofiler"]) # Ensure gprofiler stops
    gprofiler_proc.wait()
    gprofiler_log.close()

    # Assertions
    assert found_profile, f"No profile data found in {OUTPUT_DIR}"
