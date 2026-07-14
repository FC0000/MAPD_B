import json
import time
from collections import deque
import matplotlib.pyplot as plt
import numpy as np
from kafka import KafkaConsumer

# =============================================================================
# Configuration
# =============================================================================
TOPIC = "topic_results"
BOOTSTRAP = "localhost:9092"

FREQ_MIN_HZ = -1.1e6
FREQ_MAX_HZ = 1.1e6
POWER_MIN = 1e-4
POWER_MAX = 30

BENCHMARK_FILE = "benchmark_try1.json"


# =============================================================================
# Kafka consumer
# =============================================================================
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    client_id="dashboard",     
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest", # Ignore old data, read only new incoming messages
    security_protocol="PLAINTEXT"
)

# =============================================================================
# Dashboard Setup
# =============================================================================
plt.ion() # Enable matplotlib interactive mode for real-time plotting
fig = plt.figure(figsize=(15, 7))

# Set up the main global plot (top half of the window)
ax_global = plt.subplot2grid((2, 1), (0, 0))
global_line, = ax_global.plot([], [], lw=2)
ax_global.set_xlabel("Frequency (Hz)")
ax_global.set_ylabel("Power")
ax_global.set_xlim(FREQ_MIN_HZ, FREQ_MAX_HZ)
ax_global.set_ylim(POWER_MIN, POWER_MAX)
ax_global.grid(True)

worker_states = {} # Stores data and logs for each worker
worker_plots = {}  # Stores matplotlib objects for each worker's subplot

# Variables to keep track of the cumulative spectrum across all workers
global_frequencies = None
global_power_means = None
global_power_M2s = None
global_n_averaged_scans = 0

# =============================================================================
# Main loop
# ============================================================================= 
print(f"Listening for messages: latencies will be saved to {BENCHMARK_FILE}")

n_eof = 0
plot_start_time = None

throughput = None
n_scans_per_batch = None

benchmark_records = []

class eof_exit(Exception):
    pass

try:
    # Keep running as long as the matplotlib window is open
    while plt.fignum_exists(fig.number):
        
        # Fetch new messages (max 100ms wait)
        records = consumer.poll(timeout_ms=100)

        for _, messages in records.items():
            for msg in messages:
                eof = False
                receive_ts = time.time()

                if msg.headers:
                    for key, value in msg.headers:
                        if key == "EOF":
                            print("EOF")
                            eof = True
                            n_eof += 1
                if n_eof != 0 and n_eof == n_workers:
                    print(f"Received {n_eof} EOFs, exiting")
                    raise eof_exit()
                if eof:
                     continue

                packet = msg.value
    
                worker_id = packet["worker_id"]
                
                # Register a new worker if seen for the first time
                if worker_id not in worker_states:
                    worker_states[worker_id] = {
                        "frequencies": None,
                        "power_means": None,
                        "power_M2s": None,
                        "n_averaged_scans": 0,

                        "producer_tss": None,
                        "receive_tss": [],
                        "waiting_times": None,
                        "processing_times": None,
                        
                        "log": deque(maxlen=8), # Keep only the last 8 log lines
                    }
                worker = worker_states[worker_id]

                # Extract results from the packet
                results = packet["results"]
                worker["n_averaged_scans"] = results["n_averaged_scans"]
                worker["frequencies"] = np.asarray(results["frequencies"])
                worker["power_means"] = np.asarray(results["power_means"])
                worker["power_M2s"] = np.asarray(results["power_M2s"])

                # Extract timing metrics
                metrics = packet["batches_details"]
                worker["producer_tss"] = metrics["producer_timestamps"]
                worker["waiting_times"] = metrics["waiting_times"]
                worker["processing_times"] = metrics["processing_times"]
                if n_scans_per_batch is None:
                    n_scans_per_batch = metrics["scans_per_batch"]
                else:
                    assert n_scans_per_batch == metrics["scans_per_batch"]
                if throughput is None:
                    throughput = metrics["throughput"]
                else:
                    assert throughput == metrics["throughput"]
                worker["receive_tss"].append(receive_ts)
            
                
                # Update global cumulative spectrum using a running average
                if global_power_means is None:
                    if plot_start_time is None:
                        plot_start_time = time.time()
                    global_frequencies = worker["frequencies"]
                    global_power_means = worker["power_means"].copy()
                    global_power_M2s = worker["power_M2s"].copy()
                    global_n_averaged_scans = worker["n_averaged_scans"]
                else:
                    # Compute the difference and update weights based on scans
                    delta = worker["power_means"] - global_power_means
                    total_scans = global_n_averaged_scans + worker["n_averaged_scans"]
                    global_power_means += delta * worker["n_averaged_scans"] / total_scans
                    global_power_M2s += worker["power_M2s"] + delta**2 * global_n_averaged_scans * worker["n_averaged_scans"] / total_scans
                    global_n_averaged_scans = total_scans

                # Parse latencies and save benchmarks
                producer_tss = metrics.get("producer_timestamps", [])
                processing_times_s = metrics.get("processing_times", [])
                

                net_latencies_ms = []
                for t_start in producer_tss:
                    if t_start is not None:
                        # Network latency = Total time (producer-consumer) - Time spent in the network and VMs
                        network_latency = (receive_ts - t_start) * 1000
                        net_latencies_ms.append(network_latency)

                fft_latencies = [t * 1000 for t in processing_times_s]

                if net_latencies_ms:
                    worker["last_net_mean"] = np.mean(net_latencies_ms)
                    worker["last_net_p95"] = np.percentile(net_latencies_ms, 95)
                else:
                    worker["last_net_mean"] = 0.0
                    worker["last_net_p95"] = 0.0

                if fft_latencies:
                    worker["last_fft_mean"] = np.mean(fft_latencies)
                    worker["last_fft_p95"] = np.percentile(fft_latencies, 95)
                else:
                    worker["last_fft_mean"] = 0.0
                    worker["last_fft_p95"] = 0.0

                # Save Benchmark
                benchmark_records.append({
                        "worker_id": worker_id,
                        "n_averaged_scans": worker["n_averaged_scans"],
                        "production_tss": worker["producer_tss"],
                        "receive_tss": worker["receive_tss"],
                        "net_latencies_ms": net_latencies_ms,
                        "fft_latencies_ms": fft_latencies,
                        "waiting_times": worker["waiting_times"]
                    })
                    

                # Print on the dashboard
                worker["log"].appendleft(
                    f"{time.strftime('%H:%M:%S')} | "
                    f"Net [Mean: {worker['last_net_mean']:.1f}ms, P95: {worker['last_net_p95']:.1f}ms] | "
                    f"FFT [Mean: {worker['last_fft_mean']:.1f}ms, P95: {worker['last_fft_p95']:.1f}ms]"
                )

                # Print on the console
                print(
                    f"{time.strftime('%H:%M:%S')} worker={worker_id} scans={worker['n_averaged_scans']} | "
                    f"Net (mean={worker['last_net_mean']:.1f}ms, p95={worker['last_net_p95']:.1f}ms) | "
                    f"FFT (mean={worker['last_fft_mean']:.1f}ms, p95={worker['last_fft_p95']:.1f}ms)"
                )

        # -------------------------------------------------------------------------
        # Interface rendering
        # -------------------------------------------------------------------------
        # Dynamically rebuild the figure if the number of workers changes
        if len(worker_plots) != len(worker_states):
            fig.clf()
            n_workers = max(len(worker_states), 1)
            # Create a layout: 1 top row for global, 1 middle row for spectra, 1 bottom row for text logs
            grid = fig.add_gridspec(3, n_workers, height_ratios=[2.0, 0.9, 1.2], hspace=0.35)

            ax_global = fig.add_subplot(grid[0, :])
            global_line, = ax_global.plot([], [], lw=2)
            global_error = ax_global.errorbar([], [], yerr=[], fmt="none", capsize=2, alpha=0.7 )
            ax_global.set_xlim(FREQ_MIN_HZ, FREQ_MAX_HZ)
            ax_global.set_ylim(POWER_MIN, POWER_MAX)
            ax_global.set_yscale("log")
            ax_global.set_xlabel("Frequency (Hz)")
            ax_global.set_ylabel("Power")
            ax_global.grid(True)

            worker_plots = {}
            for column, w_id in enumerate(sorted(worker_states)):
                # Setup individual worker spectrum plot
                spectrum_axis = fig.add_subplot(grid[1, column])
                spectrum_line, = spectrum_axis.plot([], [], lw=1.5)
                spectrum_axis.set_xlim(FREQ_MIN_HZ, FREQ_MAX_HZ)
                spectrum_axis.set_ylim(POWER_MIN, POWER_MAX)
                spectrum_axis.set_yscale("log")
                spectrum_axis.grid(True)
                spectrum_axis.set_xlabel("Frequency (Hz)")
                spectrum_axis.set_ylabel("Power")

                # Setup text box for worker logs
                log_axis = fig.add_subplot(grid[2, column])
                log_axis.axis("off")
                log_text = log_axis.text(0, 1, "", transform=log_axis.transAxes, va="top", fontsize=8)

                worker_plots[w_id] = {
                    "spectrum_axis": spectrum_axis,
                    "spectrum_line": spectrum_line,
                    "log_text": log_text
                }

        # Draw the latest data on the global plot
        if global_power_means is not None:
            global_power_safe = np.maximum(global_power_means, 1e-12)

            global_line.set_data(global_frequencies, global_power_safe)

            # M2 -> standard deviation
            if global_n_averaged_scans > 1:
                variance = global_power_M2s / (global_n_averaged_scans - 1)

                sigma = np.sqrt(variance)
                global_error.remove()
                global_error = ax_global.errorbar(global_frequencies, global_power_safe, yerr=sigma, fmt="none", capsize=2, alpha=0.7)

            elapsed_time = time.time() - plot_start_time

            ax_global.set_title(f"Cumulative Mean Spectrum ({global_n_averaged_scans} scans, elapsed {elapsed_time:.1f} s)")

        current_time = time.time()
        
        # Iterate over all workers to update their subplots and text logs
        for w_id in sorted(worker_states):
            worker = worker_states[w_id]
            plot = worker_plots[w_id]

            #plot["spectrum_line"].set_data(worker["frequencies"], worker["power_means"])
            plot["spectrum_axis"].set_title(f"Worker {w_id}")

            freqs = worker["frequencies"]
            power = worker["power_means"]
            power = np.maximum(power, 1e-12)

            freqs = worker["frequencies"]
            power = np.maximum(worker["power_means"], 1e-12)
            plot["spectrum_line"].set_data(freqs, power)

            age_seconds = current_time - worker["receive_tss"][-1]
            
            log_lines = [
                f"Total scans: {worker['n_averaged_scans']}",
                f"Age: {age_seconds:.1f} s",
            ]

            # Calculate average time between received updates
            if len(worker["receive_tss"]) < 2:
                log_lines.append("Avg update: --")
            else:
                log_lines.append(f"Avg update: {np.mean(np.diff(worker['receive_tss'])):.2f} s")

            log_lines.extend([
                "",
                "Recent updates (Net & FFT Latency)",
                "-" * 38
            ])
            log_lines.extend(worker["log"])

            # Refresh the text area
            plot["log_text"].set_text("\n".join(log_lines))

        # Render the updated graphics to the screen
        fig.canvas.draw_idle()
        fig.canvas.flush_events()
except eof_exit:
    pass
finally:
    finish_ts = time.time()

    benchmark_output = {
            "throughput": throughput,
            "n_scans_per_batch": n_scans_per_batch,
            "finish_ts": finish_ts,
            "analysis_time": elapsed_time,
            "data": benchmark_records
    }

    with open(BENCHMARK_FILE, "w") as f:
        json.dump(benchmark_output, f, indent=2)

    print(f"Benchmark written to {BENCHMARK_FILE}")

    # Close the Kafka connection
    consumer.close()

input("Press Enter to exit...")