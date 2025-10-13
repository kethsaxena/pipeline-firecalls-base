import subprocess, os, signal, sys

result = subprocess.run([
    "docker", "run", "-d", "--rm",
    "-p", "4040:4040",
    "-v", f"{os.getcwd()}:/opt/workdir",
    "-w", "/opt/workdir",
    "apache/spark-py:v3.4.0",
    "/bin/bash", "-c", "/opt/spark/bin/spark-submit hello_sparkSQL.py"
], capture_output=True, text=True)

container_id = result.stdout.strip()
print(f"[SUCCESS:] Spark started. Container ID: {container_id}")
print("[SUCCESS:] Spark UI: http://localhost:4040\n")
print("Press Ctrl+C to stop Spark...\n")

# Handle Ctrl+C cleanly
def stop_container(sig, frame):
    print("\n🛑 Stopping Spark container...")
    subprocess.run(["docker", "stop", container_id])
    print("✅ Spark stopped.")
    sys.exit(0)

#Offical Keyboard Interrupt signal Handler
signal.signal(signal.SIGINT, stop_container)

# Stream only stdout (driver program output)
try:
    logs = subprocess.Popen(["docker", "logs", "-f", container_id],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True)
    
    ignore_keywords = ["INFO", "WARN", "DEBUG"]
    for line in logs.stdout:
        line = line.rstrip()  # optional: remove trailing newline
        contains_keyword = any(keyword in line for keyword in ignore_keywords)
        starts_with_plus = line.startswith("+")
        is_special_plus = line.startswith("+--")

        if not (contains_keyword or (starts_with_plus and not is_special_plus)):
            print(line)
        
except KeyboardInterrupt:
    stop_container(None, None)
