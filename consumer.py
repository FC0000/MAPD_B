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
POWER_MIN = 0
POWER_MAX = 20

BENCHMARK_FILE = "benchmark_latencies.json"


# =============================================================================
# Kafka consumer
# =============================================================================
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    client_id = f"dashboard", # Appears in server-side logs
    group_id=None,             # start fresh
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    security_protocol = "PLAINTEXT"
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

global_frequencies = None
global_power_means = None
global_power_M2s = None
global_n_averaged_scans = 0

# =============================================================================
# Main loop
# =============================================================================
print(f"Listening for messages: latencies will be saved to {BENCHMARK_FILE}")

try:
    while plt.fignum_exists(fig.number):
        records = consumer.poll(timeout_ms=100)

        for _, messages in records.items():
            for msg in messages:
                receive_ts = time.time()

                packet = msg.value
    
                worker_id = packet["worker_id"]
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
                        
                        "log": deque(maxlen=8), 

                        #"update_intervals": deque(maxlen=10),
                        
                        #"last_net_p95": None,
                        #"last_fft_p95": None
                    }
                worker = worker_states[worker_id]

                results = packet["results"]
                worker["n_averaged_scans"] = results["n_averaged_scans"]
                worker["frequencies"] = np.asarray(results["frequencies"])
                worker["power_means"] = np.asarray(results["power_means"])
                worker["power_M2s"] = np.asarray(results["power_M2s"])


                metrics = packet["batches_details"]
                worker["producer_tss"] = metrics["producer_timestamps"]
                worker["waiting_times"] = metrics["waiting_times"]
                worker["processing_times"] = metrics["processing_times"]
                worker["receive_tss"].append(receive_ts)

                worker["log"].appendleft(
                    f"{time.strftime('%H:%M:%S')} "
                    f"scans={worker['n_averaged_scans']}, "
                    f"producer: [{', '.join(f'{x:.1f}' for x in worker['producer_tss'])}], "
                    f"cpu-time: {np.sum(worker['processing_times']):.1f}ms, "
                    f"waits: [{', '.join(f'{x:.1f}' for x in worker['waiting_times'])}]s"
                )
                print(
                    f"{time.strftime('%H:%M:%S')} "
                    f"scans={worker['n_averaged_scans']}, "
                    f"producer: [{', '.join(f'{x:.1f}' for x in worker['producer_tss'])}], "
                    f"cpu-time: {np.sum(worker['processing_times']):.1f}ms, "
                    f"waits: [{', '.join(f'{x:.1f}' for x in worker['waiting_times'])}]s"
                )

                # update global plot data
                if global_power_means is None:
                    global_frequencies = worker["frequencies"]
                    global_power_means = worker["power_means"].copy()
                    global_power_M2s = worker["power_M2s"].copy()
                    global_n_averaged_scans = worker["n_averaged_scans"]
                else:
                    delta = worker["power_means"] - global_power_means
                    total_scans = global_n_averaged_scans + worker["n_averaged_scans"]
                    global_power_means += delta * worker["n_averaged_scans"] / total_scans
                    global_power_M2s += worker["power_M2s"] + delta**2 * global_n_averaged_scans * worker["n_averaged_scans"] / total_scans
                    global_n_averaged_scans = total_scans

                        
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

        if global_power_means is not None:
            global_line.set_data(global_frequencies, global_power_means)
            ax_global.set_title(f"Cumulative Mean Spectrum ({global_n_averaged_scans} scans)")

        current_time = time.time()
        for w_id in sorted(worker_states):
            worker = worker_states[w_id]
            plot = worker_plots[w_id]

            plot["spectrum_line"].set_data(worker["frequencies"], worker["power_means"])
            plot["spectrum_axis"].set_title(f"Worker {w_id}")

            age_seconds = current_time - worker["receive_tss"][-1]
            
            log_lines = [
                f"Total scans: {worker['n_averaged_scans']}",
                f"Age: {age_seconds:.1f} s",
            ]

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

            plot["log_text"].set_text("\n".join(log_lines))

        fig.canvas.draw_idle()
        fig.canvas.flush_events()
finally:
    consumer.close()