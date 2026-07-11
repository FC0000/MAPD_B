import json
import time
from collections import deque
import matplotlib.pyplot as plt
import numpy as np
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

# =============================================================================
# Configuration
# =============================================================================
TOPIC = "topic_results"
BOOTSTRAP = "localhost:9092"

FREQ_MIN_HZ = -1.1e6
FREQ_MAX_HZ = 1.1e6
POWER_MIN = 0
POWER_MAX = 20

BENCHMARK_FILE = "benchmark_latencies.json"

# =============================================================================
# Refresh output topic
# =============================================================================
print("Initializing...")
kafka_admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
try:
    kafka_admin.delete_topics([TOPIC])
except Exception:
    pass
kafka_admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
print(f"Created topic '{TOPIC}'.")

# =============================================================================
# Kafka consumer
# =============================================================================
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="latest",
    group_id="dashboard",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# =============================================================================
# Dashboard Setup
# =============================================================================
plt.ion()
fig = plt.figure(figsize=(15, 7))

ax_global = plt.subplot2grid((2, 1), (0, 0))
global_line, = ax_global.plot([], [], lw=2)
ax_global.set_xlabel("Frequency (Hz)")
ax_global.set_ylabel("Power")
ax_global.set_xlim(FREQ_MIN_HZ, FREQ_MAX_HZ)
ax_global.set_ylim(POWER_MIN, POWER_MAX)
ax_global.grid(True)

worker_states = {}
worker_plots = {}

global_frequency = None
global_power_mean = None
global_power_M2 = None
global_num_scans = 0

# =============================================================================
# Main loop
# =============================================================================
print(f"Listening for messages: latencies will be saved to {BENCHMARK_FILE}")


while plt.fignum_exists(fig.number):
    records = consumer.poll(timeout_ms=100)

    for _, messages in records.items():
        for msg in messages:
            receive_ts = time.time()

            packet = msg.value
                    
            worker_id = packet["worker_id"]
            if worker_id not in worker_states:
                worker_states[worker_id] = {
                    "frequency": None,
                    "power_mean": None,
                    "power_M2": None,
                    "num_scans": 0,

                    "producer_tss": None,
                    "fft_latencies_ms": None,
                    "receive_tss": [],

                    "log": deque(maxlen=8), 

                    #"update_intervals": deque(maxlen=10),
                    
                    #"last_net_p95": None,
                    #"last_fft_p95": None
                }
            worker = worker_states[worker_id]

            worker["num_scans"] = packet["num_averaged_scans"]
                    
            average = packet["average"]
            worker["frequency"] = np.asarray(average["frequency"])
            worker["power_mean"] = np.asarray(average["power_mean"])
            worker["power_M2"] = np.asarray(average["power_M2"])

            metrics = packet["metrics"]
            last_producer_ts = metrics["producer_tss"][-1]
            worker["producer_tss"] = last_producer_ts # only take last producer timestamp
            worker["fft_latencies_ms"] = metrics["fft_latencies_ms"]

            worker["receive_tss"].append(receive_ts)

            worker["log"].appendleft(f"{time.strftime('%H:%M:%S')} scans={worker["num_scans"]}, tot-latency: {receive_ts - last_producer_ts:.1f}s, cpu-time: {np.sum(worker["fft_latencies_ms"]):.1f}ms")
            print(f"{time.strftime('%H:%M:%S')} worker={worker_id} scans={worker["num_scans"]}, tot-latency: {receive_ts - last_producer_ts:.1f}s, cpu-time: {np.sum(worker["fft_latencies_ms"]):.1f}ms")

            # update global plot data
            if global_power_mean is None:
                global_frequency = worker["frequency"]
                global_power_mean = worker["power_mean"].copy()
                global_power_M2 = worker["power_M2"].copy()
                global_num_scans = worker["num_scans"]
            else:
                delta = worker["power_mean"] - global_power_mean
                total_scans = global_num_scans + worker["num_scans"]
                global_power_mean += delta * worker["num_scans"] / total_scans
                global_power_M2 += worker["power_M2"] + delta**2 * global_num_scans * worker["num_scans"] / total_scans
                global_num_scans = total_scans

                    
            """packet_timings = metrics.get("packet_timings", [])
                    fft_latencies = metrics.get("fft_latencies_ms", [])
                    
                    net_latencies_ms = []
                    for t_info in packet_timings:
                        t_start = t_info.get("producer_ts")
                        t_cloud = t_info.get("time_in_cloud_ms")
                        if t_start is not None and t_cloud is not None:
                            total_deltat = (t_now_local - t_start) * 1000
                            network_latency = total_deltat - t_cloud
                            net_latencies_ms.append(network_latency)

                    if net_latencies_ms:
                        worker["last_net_p95"] = np.percentile(net_latencies_ms, 95)
                    if fft_latencies:
                        worker["last_fft_p95"] = np.percentile(fft_latencies, 95)

                    if net_latencies_ms or fft_latencies:
                        benchmark_record = {
                            "timestamp": t_now_local,
                            "worker_id": worker_id,
                            "num_scans": num_scans,
                            "net_latencies_ms": net_latencies_ms,
                            "fft_latencies_ms": fft_latencies
                        }
                        with open(BENCHMARK_FILE, "a") as f:
                            f.write(json.dumps(benchmark_record) + "\n")

                    worker["log"].appendleft(
                        f"{time.strftime('%H:%M:%S')} | "
                        f"Net P95: {worker['last_net_p95']:.1f}ms | "
                        f"FFT P95: {worker['last_fft_p95']:.1f}ms"
                    )

                    print(f"{time.strftime('%H:%M:%S')} worker={worker_id} scans={num_scans} "
                        f"Net_P95={worker['last_net_p95']:.1f}ms")

            """

    # -------------------------------------------------------------------------
    # Interface rendering
    # -------------------------------------------------------------------------
    if len(worker_plots) != len(worker_states):
        fig.clf()
        n_workers = max(len(worker_states), 1)
        grid = fig.add_gridspec(3, n_workers, height_ratios=[2.0, 0.9, 1.2], hspace=0.35)

        ax_global = fig.add_subplot(grid[0, :])
        global_line, = ax_global.plot([], [], lw=2)
        ax_global.set_xlim(FREQ_MIN_HZ, FREQ_MAX_HZ)
        ax_global.set_ylim(POWER_MIN, POWER_MAX)
        ax_global.set_xlabel("Frequency (Hz)")
        ax_global.set_ylabel("Power")
        ax_global.grid(True)

        worker_plots = {}
        for column, w_id in enumerate(sorted(worker_states)):
            spectrum_axis = fig.add_subplot(grid[1, column])
            spectrum_line, = spectrum_axis.plot([], [], lw=1.5)
            spectrum_axis.set_xlim(FREQ_MIN_HZ, FREQ_MAX_HZ)
            spectrum_axis.set_ylim(POWER_MIN, POWER_MAX)
            spectrum_axis.grid(True)
            spectrum_axis.set_xlabel("Frequency (Hz)")
            spectrum_axis.set_ylabel("Power")

            log_axis = fig.add_subplot(grid[2, column])
            log_axis.axis("off")
            log_text = log_axis.text(0, 1, "", transform=log_axis.transAxes, va="top", fontsize=8)

            worker_plots[w_id] = {
                "spectrum_axis": spectrum_axis,
                "spectrum_line": spectrum_line,
                "log_text": log_text
            }

    if global_power_mean is not None:
        global_line.set_data(global_frequency, global_power_mean)
        ax_global.set_title(f"Cumulative Mean Spectrum ({global_num_scans} scans)")

    current_time = time.time()
    for w_id in sorted(worker_states):
        worker = worker_states[w_id]
        plot = worker_plots[w_id]

        plot["spectrum_line"].set_data(worker["frequency"], worker["power_mean"])
        plot["spectrum_axis"].set_title(f"Worker {w_id}")

        age_seconds = current_time - worker["receive_tss"][-1]
        
        log_lines = [
            f"Total scans: {worker['num_scans']}",
            f"Age: {age_seconds:.1f} s",
        ]

        if worker["receive_tss"] is None:
            log_lines.append("Avg update: --")
        else:
            log_lines.append(f"Avg update: {np.mean(np.diff(worker['receive_tss'])):.2f} s")

        log_lines.extend([
            "",
            "Recent updates (Net & FFT Latency)",
            "-" * 38
        ])
        log_lines.extend(worker["log"])

        plot["log_text"].set_text("\n".join(log_lines))

    fig.canvas.draw_idle()
    fig.canvas.flush_events()