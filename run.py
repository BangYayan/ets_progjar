#!/usr/bin/env python3

import subprocess
import time
import json
import csv
import os
import signal
import sys
from pathlib import Path

# Configuration
PYTHON_CMD = "python3"
SERVER_THREAD_SCRIPT = "file_server_thread.py"
SERVER_PROCESS_SCRIPT = "file_server_process.py"
STRESS_TEST_SCRIPT = "stress_test.py"
RESULTS_CSV = "stress_test_results.csv"
SERVER_ADDRESS = "localhost:9999"
CLIENT_TIMEOUT = 30000
SERVER_LOG_DIR = "server_logs"

# Test configurations
operations = ["upload", "upload", "upload", "upload", "upload", "upload", "upload", "upload", "upload", 
              "download", "download", "download", "download", "download", "download", "download", "download", "download"]
file_sizes = [10, 10, 10, 50, 50, 50, 100, 100, 100, 10, 10, 10, 50, 50, 50, 100, 100, 100]
client_workers_list = [1, 5, 50, 1, 5, 50, 1, 5, 50, 1, 5, 50, 1, 5, 50, 1, 5, 50]
server_types = ["thread", "process"]
server_worker_counts = [1, 5, 50]
fieldnames = ["test_id", "operation", "file_size_mb", "client_workers", "server_type", "server_workers", 
              "total_time", "throughput_bytes_sec", "client_success", "client_failed", "server_status"]


def cleanup_previous_servers():
    try:
        # Kill thread-based servers
        subprocess.run(["pkill", "-f", f"{PYTHON_CMD} {SERVER_THREAD_SCRIPT}"], 
                      stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        # Kill process-based servers
        subprocess.run(["pkill", "-f", f"{PYTHON_CMD} {SERVER_PROCESS_SCRIPT}"], 
                      stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        time.sleep(1)
    except FileNotFoundError:
        pass


def start_server(server_type, workers, test_id_for_log):
    Path(SERVER_LOG_DIR).mkdir(exist_ok=True)
    server_log_file = f"{SERVER_LOG_DIR}/server_test_{test_id_for_log}_{server_type}_{workers}.log"
    
    if server_type == "thread":
        cmd = [PYTHON_CMD, SERVER_THREAD_SCRIPT, str(workers)]
    else:
        cmd = [PYTHON_CMD, SERVER_PROCESS_SCRIPT, str(workers)]
    
    print(f"  [DEBUG SERVER] Starting server with command: {' '.join(cmd)}", file=sys.stderr)
    print(f"  [DEBUG SERVER] Server stdout/stderr for this test will be logged to: {server_log_file}", file=sys.stderr)
    
    try:
        with open(server_log_file, 'w') as log_file:
            process = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        
        time.sleep(0.8)
        
        if process.poll() is not None:
            print(f"  [DEBUG SERVER] Server PID {process.pid} NOT FOUND shortly after start. Check {server_log_file} for errors.", file=sys.stderr)
            if os.path.exists(server_log_file):
                print(f"  [DEBUG SERVER] Last lines from {server_log_file}:", file=sys.stderr)
                try:
                    with open(server_log_file, 'r') as f:
                        lines = f.readlines()
                        for line in lines[-5:]:
                            print(f"    {line.rstrip()}", file=sys.stderr)
                except Exception as e:
                    print(f"    Error reading log file: {e}", file=sys.stderr)
            return None
        else:
            print(f"  [DEBUG SERVER] Server PID {process.pid} was created.", file=sys.stderr)
            return process.pid
            
    except Exception as e:
        print(f"  [DEBUG SERVER] Error starting server: {e}", file=sys.stderr)
        return None


def run_client_test(operation, server_addr, file_size, workers, use_process_pool=False):
    """Run client stress test and return results"""
    cmd = [PYTHON_CMD, STRESS_TEST_SCRIPT, "--server", server_addr, "--operation", operation, 
           "--file-size", str(file_size), "--workers", str(workers)]
    
    if use_process_pool:
        cmd.append("--use-process-pool")
    
    print(f"  [DEBUG CLIENT] Running client test with command: {' '.join(cmd)}", file=sys.stderr)
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=CLIENT_TIMEOUT)
        exit_code = result.returncode
        output = result.stdout
        
        print(f"  [DEBUG CLIENT] Client test exit code: {exit_code}", file=sys.stderr)
        
        if exit_code == 0:
            return output, True
        else:
            print(f"  [ERROR CLIENT] Output: {result.stderr}", file=sys.stderr)
            return "CLIENT_TEST_FAILED", False
            
    except subprocess.TimeoutExpired:
        print(f"  [DEBUG CLIENT] Client test timed out after {CLIENT_TIMEOUT} seconds", file=sys.stderr)
        return "CLIENT_TEST_TIMED_OUT", False
    except Exception as e:
        print(f"  [ERROR CLIENT] Exception running client test: {e}", file=sys.stderr)
        return "CLIENT_TEST_FAILED", False


def is_process_running(pid):
    """Check if a process with given PID is running"""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def kill_process(pid):
    """Kill a process by PID"""
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(0.5)
        # Check if still running, force kill if necessary
        if is_process_running(pid):
            os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def parse_json_result(json_str):
    """Parse JSON result from client test"""
    try:
        data = json.loads(json_str)
        return {
            'total_time': data.get('total_time', 'json_err'),
            'throughput_bytes_sec': data.get('throughput', 'json_err'),
            'client_success': data.get('successful', 'json_err'),
            'client_failed': data.get('failed', 'json_err')
        }
    except (json.JSONDecodeError, AttributeError):
        return {
            'total_time': 'json_err',
            'throughput_bytes_sec': 'json_err',
            'client_success': 'json_err',
            'client_failed': 'json_err'
        }


def main():
    """Main stress test runner"""
    # Initialize CSV file
    Path(SERVER_LOG_DIR).mkdir(exist_ok=True)
    
    with open(RESULTS_CSV, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    
    test_id = 1
    
    # Run all test combinations
    for i, operation in enumerate(operations):
        file_size = file_sizes[i]
        c_workers = client_workers_list[i]
        
        for s_type in server_types:
            for s_workers in server_worker_counts:
                print(f"\n--- Test {test_id}: {operation} {file_size}MB, {c_workers} clients, Server: {s_type} ({s_workers} workers) ---")
                
                print("  [DEBUG MAIN] Cleaning previous servers...", file=sys.stderr)
                cleanup_previous_servers()
                
                print(f"  [DEBUG MAIN] Starting server ({s_type}, {s_workers} workers) for Test {test_id}...", file=sys.stderr)
                server_pid = start_server(s_type, s_workers, test_id)
                
                # Initialize CSV row data
                csv_row = {
                    'test_id': test_id,
                    'operation': operation,
                    'file_size_mb': file_size,
                    'client_workers': c_workers,
                    'server_type': s_type,
                    'server_workers': s_workers
                }
                
                if server_pid is None:
                    print(f"  [ERROR MAIN] Failed to start server for Test {test_id}. Skipping.", file=sys.stderr)
                    csv_row.update({
                        'total_time': 'NA',
                        'throughput_bytes_sec': 'NA',
                        'client_success': 'NA',
                        'client_failed': 'NA',
                        'server_status': 'SERVER_START_FAILED'
                    })
                    
                    # Write error result
                    with open(RESULTS_CSV, 'a', newline='') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writerow(csv_row)
                    
                    test_id += 1
                    continue
                
                print(f"  [DEBUG MAIN] Server started with PID: {server_pid} for Test {test_id}. Waiting for warm-up...", file=sys.stderr)
                time.sleep(2)
                
                print(f"  [DEBUG MAIN] Running client test for Test {test_id}...", file=sys.stderr)
                client_result, client_success = run_client_test(operation, SERVER_ADDRESS, file_size, c_workers, False)
                
                print(f"  [DEBUG MAIN] run_client_test finished. Success: {client_success}. Result: '{client_result}'", file=sys.stderr)
                
                if not client_success:
                    csv_row.update({
                        'total_time': '',
                        'throughput_bytes_sec': '',
                        'client_success': '',
                        'client_failed': '',
                        'server_status': f'ERROR_CLIENT_{client_result}'
                    })
                else:
                    # Parse JSON results
                    parsed_results = parse_json_result(client_result)
                    csv_row.update(parsed_results)
                
                # Check server status
                current_client_error_status = csv_row.get('server_status', '')
                
                if is_process_running(server_pid):
                    if current_client_error_status:
                        csv_row['server_status'] = f"{current_client_error_status}_SrvRUN"
                    else:
                        csv_row['server_status'] = "OK"
                    
                    print(f"  [DEBUG MAIN] Server PID {server_pid} running. Terminating...", file=sys.stderr)
                    kill_process(server_pid)
                else:
                    print(f"  [WARN MAIN] Server PID {server_pid} NOT running after client test.", file=sys.stderr)
                    if current_client_error_status:
                        csv_row['server_status'] = f"{current_client_error_status}_SrvCRASH"
                    else:
                        csv_row['server_status'] = "CRASHED"
                
                # Ensure all fields have values
                for field in fieldnames:
                    if field not in csv_row or csv_row[field] is None:
                        csv_row[field] = 'NA'
                
                time.sleep(1)
                
                print(f"  [DEBUG MAIN] CSV line: {csv_row}", file=sys.stderr)
                
                # Write results to CSV
                with open(RESULTS_CSV, 'a', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(csv_row)
                
                print(f"--- Finished Test {test_id} ---")
                test_id += 1
    
    print(f"\nStress test complete. Results: {RESULTS_CSV}. Server logs: {SERVER_LOG_DIR}")


if __name__ == "__main__":
    main()