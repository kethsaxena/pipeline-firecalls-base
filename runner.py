import subprocess, os, signal, sys

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

def runSpark(spark_job_path):
    result = subprocess.run([
        "docker", "run", "-d", "--rm",
        "-p", "4040:4040",
        "-v", f"{os.getcwd()}:/opt/workdir",
        "-w", "/opt/workdir",
        "-e", "PYTHONPATH=/opt/workdir",
        "apache/spark-py:v3.4.0",
        "/bin/bash", "-c", f"/opt/spark/bin/spark-submit {spark_job_path}"
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

def main():
    if len(sys.argv) < 2:
        print("Usage: python runner.py <job_type> <job_name>")
        print("Example: python runner.py spark hello_sparkSQL.py")
        sys.exit(1)

    job_type = sys.argv[1].lower()      # 'spark' or 'pandas'
    job_name = sys.argv[2]              # script filename

    if job_type == "spark":
        print(f"DEBUG: Mounting local dir: {os.getcwd()} to /opt/workdir in container")
        spark_job_path = f"processor/spark/{job_name}"
        runSpark(spark_job_path)
    elif job_type == "pandas":
        # Path to the Python in the current venv
        python_executable = sys.executable

        pandas_job_path = os.path.join(PROJECT_ROOT, "processor", "pandas", job_name)

        # Ensure PYTHONPATH includes project root
        env = os.environ.copy()
        env["PYTHONPATH"] = PROJECT_ROOT

        # Run pandas job using venv Python
        result = subprocess.run(
            [python_executable, pandas_job_path],
            cwd=PROJECT_ROOT,
            env=env,
            check=True
        )
        print(f"[SUCCESS] Pandas job '{job_name}' finished with return code {result.returncode}")

if __name__ == "__main__":
    main()
