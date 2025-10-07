import json
import os
import re
import time
import subprocess
from utils import *
from atomix_setup import atomix_setup


def parse_metrics(output):
    metrics = {}
    # Find the metrics section between METRICS_START and METRICS_END
    metrics_section = re.search(r"METRICS_START\n(.*?)\nMETRICS_END", output, re.DOTALL)
    if not metrics_section:
        print("Warning: No metrics section found in output")
        return {"throughput": 0.0}

    metrics_text = metrics_section.group(1)

    # Regular expressions to match the metrics
    patterns = {
        "throughput": r"Throughput: ([\d\.]+) transactions/second",
        "avg_latency": r"Average Latency: ([\d\.]+[µnm]?s)",
        "p50_latency": r"P50 Latency: ([\d\.]+[µnm]?s)",
        "p95_latency": r"P95 Latency: ([\d\.]+[µnm]?s)",
        "p99_latency": r"P99 Latency: ([\d\.]+[µnm]?s)",
        "total_duration": r"Total Duration: ([\d\.]+[µnm]?s)",
        "total_transactions": r"Total Transactions: (\d+)",
        "resolver_stats": r"Resolver stats: (.*)",
        "range_server_stats": r"Range server stats: (.*)",
    }

    for metric, pattern in patterns.items():
        match = re.search(pattern, metrics_text)
        if match:
            value = match.group(1)
            # Convert duration strings to seconds
            if metric == "resolver_stats":
                metrics[metric] = value
            elif metric == "range_server_stats":
                metrics[metric] = value
            elif "s" in value:
                if "µs" in value:
                    metrics[metric] = float(value.replace("µs", "")) / 1_000_000
                elif "ms" in value:
                    metrics[metric] = float(value.replace("ms", "")) / 1_000
                elif "ns" in value:
                    metrics[metric] = float(value.replace("ns", "")) / 1_000_000_000
                else:
                    metrics[metric] = float(value.replace("s", ""))
            else:
                metrics[metric] = float(value)
    # print(metrics)
    return metrics


def run_workload(config):
    global atomix_setup

    iteration = config["iteration"]
    baseline = config["baseline"]
    resolver_background_runtime_core_ids = config["resolver_capacity"][
        "background_runtime_core_ids"
    ]
    resolver_cpu_percentage = config["resolver_capacity"]["cpu_percentage"]
    resolver_cores = config["resolver_cores"]
    resolver_tx_load = config["resolver_tx_load"]
    main_num_keys = config["num_keys"]
    main_max_concurrency = config["max_concurrency"]
    main_num_queries = config["num_queries"]
    main_name = config["name"]
    main_background_runtime_core_ids = config["background_runtime_core_ids"]
    workload_type = config["workload_type"]

    del config["iteration"]
    del config["baseline"]
    del config["resolver_capacity"]
    del config["resolver_cores"]
    del config["resolver_tx_load"]
    del config["resolver_tx_load_concurrency"]

    # Main workload generator -- used to collect performance metrics
    cmd1 = [
        TARGET_RUN_CMD + "workload-generator",
        "--workload-config",
        str(MAIN_RAY_WORKLOAD_CONFIG_PATH),
        "--config",
        str(RAY_SERVERS_CONFIG_PATH),
    ]
    # Secondary workload generator -- used to overload the resolver with txs
    cmd2 = [
        TARGET_RUN_CMD + "workload-generator",
        "--workload-config",
        str(SECONDARY_RAY_WORKLOAD_CONFIG_PATH),
        "--config",
        str(RAY_SERVERS_CONFIG_PATH),
    ]

    if iteration == 0:
        if workload_type == "ycsb":
            atomix_setup.servers_config["heuristic"] = "LockContention"
        else:
            atomix_setup.servers_config["heuristic"] = "OpenClients"
        atomix_setup.servers_config["resolver"][
            "cpu_percentage"
        ] = resolver_cpu_percentage
        atomix_setup.servers_config["resolver"][
            "background_runtime_core_ids"
        ] = resolver_background_runtime_core_ids

        # Add cascading abort config parameters if present
        if "enable_cascading_abort" in config:
            atomix_setup.servers_config["enable_cascading_abort"] = config["enable_cascading_abort"]
        if "abort_injection_rate" in config:
            atomix_setup.servers_config["abort_injection_rate"] = config["abort_injection_rate"]

        atomix_setup.dump_servers_config()
        atomix_setup.kill_servers()
        atomix_setup.reset_cassandra()
        atomix_setup.start_servers()

        # Create a temporary config file with the parameters of the main workload generator
        config["fake_transactions"] = False
        os.makedirs(os.path.dirname(MAIN_RAY_WORKLOAD_CONFIG_PATH), exist_ok=True)
        with open(MAIN_RAY_WORKLOAD_CONFIG_PATH, "w") as f:
            json.dump(config, f)
        cmd1.append("--create-keyspace")

        # Create a temporary config file with the parameters of the secondary workload generator
        config["fake_transactions"] = True
        config["workload_type"] = "custom"  # Secondary workload is always custom
        config["max_concurrency"] = resolver_tx_load["max_concurrency"]
        config["num_queries"] = resolver_tx_load["num_queries"]
        config["num_keys"] = resolver_tx_load["num_keys"]
        config["name"] = f"{main_name}-2"
        config["background_runtime_core_ids"] = resolver_tx_load[
            "background_runtime_core_ids"
        ]
        os.makedirs(os.path.dirname(SECONDARY_RAY_WORKLOAD_CONFIG_PATH), exist_ok=True)
        with open(SECONDARY_RAY_WORKLOAD_CONFIG_PATH, "w") as f:
            json.dump(config, f)
        del config["fake_transactions"]
        # cmd2.append("--create-keyspace")
        config["workload_type"] = workload_type
        config["max_concurrency"] = main_max_concurrency
        config["num_queries"] = main_num_queries
        config["num_keys"] = main_num_keys
        config["name"] = main_name
        config["background_runtime_core_ids"] = main_background_runtime_core_ids

    # Add back the config params so that they are reported by ray
    config["iteration"] = iteration
    config["baseline"] = baseline
    config["resolver_cores"] = resolver_cores
    config["resolver_tx_load_concurrency"] = resolver_tx_load["max_concurrency"]
    print("cmd1: ", cmd1)
    print("cmd2: ", cmd2)

    try:
        process2 = None
        if resolver_tx_load["max_concurrency"] != "0":
            process2 = subprocess.Popen(
                cmd2,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, "RUST_LOG": "error"},
            )

        # Let it run for a while
        time.sleep(2)

        process1 = subprocess.Popen(
            cmd1,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "RUST_LOG": "error"},
        )
        try:
            # Wait for the main workload generator to finish
            stdout1, stderr1 = process1.communicate(
                timeout=60 * 60
            )  # 60 minutes timeout
            print(stderr1)
            metrics = parse_metrics(stdout1)
            print("Finished main workload generator")

            # Send interrupt signal to the secondary workload generator
            if process2:
                process2.send_signal(subprocess.signal.SIGUSR1)
                print("Sent SIGUSR1 to secondary workload generator")
            # Wait a bit for graceful shutdown, then force kill if needed
            try:
                if process2:
                    process2.wait(timeout=500)
            except subprocess.TimeoutExpired:
                if process2:
                    process2.kill()
                print("Force killed secondary workload generator")

        except subprocess.TimeoutExpired:
            process1.kill()
            if process2:
                process2.kill()
            print(f"Timeout exceeded for config: {config}")
            metrics = {"throughput": 0.0}

    except Exception as e:
        print(f"Error running workloads: {e}")
        metrics = {"throughput": 0.0}
    return metrics
