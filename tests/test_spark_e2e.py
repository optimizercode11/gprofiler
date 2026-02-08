import os
import shutil
import subprocess
import time
import requests
import pytest
import sys
import glob
import json
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

    # 2. Build Java Agent
    agent_dir = REPO_ROOT / "runtime-agents" / "gprofiler-spark-agent"
    print("Building gprofiler-spark-agent...")
    subprocess.run(["mvn", "clean", "package", "-DskipTests"], check=True, cwd=agent_dir)
    agent_jar = list((agent_dir / "target").glob("gprofiler-spark-agent-*.jar"))[0]
    if not agent_jar.exists():
        pytest.fail("gprofiler-spark-agent jar not found!")
    os.environ["GPROFILER_SPARK_AGENT_JAR"] = str(agent_jar)

    # 3. Download Spark
    if not os.path.exists(SPARK_DIR):
        print(f"Downloading Spark {SPARK_VERSION}...")
        subprocess.run(["curl", "-O", SPARK_URL], check=True)
        subprocess.run(["tar", "-xzf", SPARK_TGZ], check=True)

    # 4. Create output directory
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
        for (int i = 0; i < 10000; i++) {
            data.add(i);
        }

        // Run for enough time to allow gprofiler to attach and capture multiple snapshots/updates
        for (int i = 0; i < 30; i++) {
            // Use 4 partitions to trigger multiple tasks (and potential thread reuse/renaming)
            JavaRDD<Integer> distData = sc.parallelize(data, 4);
            long count = distData.map(x -> x * 2).count();
            System.out.println("Iteration " + i + ": " + count);
            Thread.sleep(2000); // Sleep to extend duration
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

    agent_jar = os.environ.get("GPROFILER_SPARK_AGENT_JAR")
    if not agent_jar:
        pytest.fail("GPROFILER_SPARK_AGENT_JAR environment variable not set")

    print(f"Submitting Spark Application with agent: {agent_jar}...")
    # Using Popen for async execution
    app_proc = subprocess.Popen(
        [
            f"./{SPARK_DIR}/bin/spark-submit",
            "--class", "GProfilerTestApp",
            "--master", spark_cluster,
            "--conf", f"spark.driver.extraJavaOptions=-javaagent:{agent_jar}",
            "--conf", f"spark.executor.extraJavaOptions=-javaagent:{agent_jar}",
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
    metadata_files = []

    # Wait up to 90 seconds (Spark app setup + profiling duration)
    while time.time() - start_time < 90:
        # Check output directory for .col files (collapsed stacks) or .json
        files = list(OUTPUT_DIR.glob("*"))
        metadata_files = list(OUTPUT_DIR.glob("*_metadata*.json"))

        if len(files) > 0 and len(metadata_files) > 0:
            print(f"Found profile output: {files}")
            found_profile = True
            # Wait a bit more to allow for updates to be written
            time.sleep(10)
            # Refresh file list
            metadata_files = list(OUTPUT_DIR.glob("*_metadata*.json"))
            break
        time.sleep(2)

    # Cleanup
    app_proc.terminate()
    subprocess.run(["sudo", "pkill", "-f", "gprofiler"]) # Ensure gprofiler stops
    gprofiler_proc.wait()
    gprofiler_log.close()

    # Assertions
    assert found_profile, f"No profile data found in {OUTPUT_DIR}"
    assert len(metadata_files) > 0, f"No metadata files found in {OUTPUT_DIR}"

    # Verify metadata content
    spark_thread_found = False
    thread_names = set()

    for meta_file in metadata_files:
        print(f"Inspecting metadata file: {meta_file}")
        with open(meta_file) as f:
            try:
                data = json.load(f)
                threads = data.get("threads", {})
                for tid, name in threads.items():
                    thread_names.add(name)
                    # typical Spark executor threads
                    if "Executor task launch worker" in name or "TaskSetManager" in name:
                        print(f"Found Spark thread: {name} (TID: {tid})")
                        spark_thread_found = True
            except json.JSONDecodeError:
                print(f"Failed to decode JSON from {meta_file}")

    assert spark_thread_found, f"Did not find any Spark execution threads in metadata. Found: {thread_names}"
