import docker
import os
import time
import subprocess
import xml.etree.ElementTree as ET

# Configuration
POM_XML = os.path.abspath("runtime-agents/gprofiler-spark-agent/pom.xml")
DEMO_APP_DIR = os.path.abspath("demo_app")
HOST_PORT = 12345

def get_agent_jar_path():
    tree = ET.parse(POM_XML)
    root = tree.getroot()
    ns = {'mvn': 'http://maven.apache.org/POM/4.0.0'}
    version = root.find('mvn:version', ns).text
    artifact_id = root.find('mvn:artifactId', ns).text
    return os.path.abspath(f"runtime-agents/gprofiler-spark-agent/target/{artifact_id}-{version}.jar")

AGENT_JAR = get_agent_jar_path()

def build_agent():
    print("Building Spark Agent...")
    subprocess.run(["mvn", "-f", POM_XML, "clean", "package"], check=True)

def run_demo():
    client = docker.from_env()

    # Create a simple Java app if it doesn't exist
    if not os.path.exists(DEMO_APP_DIR):
        os.makedirs(DEMO_APP_DIR)

    with open(os.path.join(DEMO_APP_DIR, "DemoApp.java"), "w") as f:
        f.write("""
        public class DemoApp {
            public static void main(String[] args) throws Exception {
                System.out.println("Demo App started. PID: " + java.lang.management.ManagementFactory.getRuntimeMXBean().getName());
                int i = 0;
                while (true) {
                    Thread.sleep(5000);
                    System.out.println("App running... iteration " + i++);

                    // Rename thread to trigger agent logic
                    Thread.currentThread().setName("demo-thread-" + i);
                }
            }
        }
        """)

    volumes = {
        AGENT_JAR: {'bind': '/agent.jar', 'mode': 'ro'},
        DEMO_APP_DIR: {'bind': '/app', 'mode': 'rw'}
    }

    environment = {
        "GPROFILER_HOST": "127.0.0.1", # Assuming host networking
        "GPROFILER_PORT": str(HOST_PORT)
    }

    print(f"Starting Demo App Container with Agent attached...")
    print(f"Expecting backend at localhost:{HOST_PORT} (ensure you have something listening or ignore connection errors)")

    try:
        container = client.containers.run(
            "eclipse-temurin:8-jdk",
            command='sh -c "javac /app/DemoApp.java && java -javaagent:/agent.jar -cp /app DemoApp"',
            volumes=volumes,
            environment=environment,
            network_mode="host",
            detach=True
        )

        print(f"Container ID: {container.id}")
        print("Tailing logs (Ctrl+C to stop)...")

        for line in container.logs(stream=True):
            print(line.decode().strip())

    except KeyboardInterrupt:
        print("\nStopping container...")
        container.stop()
        container.remove()
    except Exception as e:
        print(f"Error: {e}")
        try:
            container.stop()
            container.remove()
        except:
            pass

if __name__ == "__main__":
    build_agent()
    run_demo()
