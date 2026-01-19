import threading
import time
import json
import pytest
import docker
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import subprocess

AGENT_JAR = os.path.abspath("runtime-agents/gprofiler-spark-agent/target/gprofiler-spark-agent-1.0-SNAPSHOT.jar")
POM_XML = os.path.abspath("runtime-agents/gprofiler-spark-agent/pom.xml")

def build_agent_jar():
    print("Building Spark Agent Jar...")
    result = subprocess.run(
        ["mvn", "-f", POM_XML, "clean", "package"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError(f"Maven build failed: {result.stderr}")
    print("Build successful.")

class MockGProfilerHandler(BaseHTTPRequestHandler):
    received_heartbeats = []
    received_threads = []
    profile_response = False

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))

            if self.path == '/spark':
                if data.get('type') == 'thread_info':
                    MockGProfilerHandler.received_threads.append(data)
                else:
                    MockGProfilerHandler.received_heartbeats.append(data)

                response = {"profile": MockGProfilerHandler.profile_response}
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response).encode('utf-8'))
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            print(f"Error in handler: {e}")

    def log_message(self, format, *args):
        pass # Silence logs

@pytest.fixture
def mock_server():
    MockGProfilerHandler.received_heartbeats = []
    MockGProfilerHandler.received_threads = []
    MockGProfilerHandler.profile_response = False

    server = ThreadingHTTPServer(('0.0.0.0', 0), MockGProfilerHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    port = server.server_port
    yield port

    server.shutdown()
    server.server_close()

def test_spark_agent_e2e(mock_server, tmp_path):
    build_agent_jar()
    if not os.path.exists(AGENT_JAR):
        pytest.fail(f"Agent jar not found at {AGENT_JAR} even after build.")

    # Create Sleep.java
    sleep_java = tmp_path / "Sleep.java"
    sleep_java.write_text("""
    public class Sleep {
        public static void main(String[] args) throws Exception {
            System.out.println("App starting");
            Thread.sleep(10000);
            System.out.println("App finishing");
        }
    }
    """)

    client = docker.from_env()

    volumes = {
        AGENT_JAR: {'bind': '/agent.jar', 'mode': 'ro'},
        str(tmp_path): {'bind': '/app', 'mode': 'rw'}
    }

    environment = {
        "GPROFILER_HOST": "127.0.0.1",
        "GPROFILER_PORT": str(mock_server)
    }

    # Enable profiling response for the first heartbeat
    MockGProfilerHandler.profile_response = True

    try:
        container = client.containers.run(
            "eclipse-temurin:8-jdk",
            command='sh -c "javac /app/Sleep.java && java -Dspark.app.id=test-app-id -Dspark.app.name=TestApp -javaagent:/agent.jar -cp /app Sleep"',
            volumes=volumes,
            environment=environment,
            network_mode="host",
            detach=True
        )
    except docker.errors.ImageNotFound:
        # Pull if not present (though run usually does this, being explicit helps)
        client.images.pull("eclipse-temurin:8-jdk")
        container = client.containers.run(
            "eclipse-temurin:8-jdk",
            command='sh -c "javac /app/Sleep.java && java -Dspark.app.id=test-app-id -Dspark.app.name=TestApp -javaagent:/agent.jar -cp /app Sleep"',
            volumes=volumes,
            environment=environment,
            network_mode="host",
            detach=True
        )


    try:
        # Wait for data
        start_time = time.time()
        while time.time() - start_time < 15:
            if MockGProfilerHandler.received_heartbeats and MockGProfilerHandler.received_threads:
                break
            time.sleep(0.5)

        print("Logs from container:")
        print(container.logs().decode())

        # Verify Heartbeat
        assert len(MockGProfilerHandler.received_heartbeats) > 0, "No heartbeat received"
        heartbeat = MockGProfilerHandler.received_heartbeats[0]
        assert heartbeat['spark.app.id'] == 'test-app-id'
        assert heartbeat['spark.app.name'] == 'TestApp'
        assert 'pid' in heartbeat

        # Verify Thread Info
        assert len(MockGProfilerHandler.received_threads) > 0, "No thread info received"
        thread_info = MockGProfilerHandler.received_threads[0]
        assert thread_info['spark.app.id'] == 'test-app-id'
        assert thread_info['type'] == 'thread_info'
        assert len(thread_info['threads']) > 0

        # Check if 'main' thread is present
        thread_names = [t['name'] for t in thread_info['threads']]
        print(f"Received threads: {thread_names}")
        assert 'main' in thread_names

    finally:
        container.stop()
        container.remove()
