import json
import time
from datetime import datetime
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


# =============================================================================
# Kafka consumer
# =============================================================================
print("Initializing Kafka consumer...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    client_id="dashboard",     
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest", # Ignore old data, read only new incoming messages
    security_protocol="PLAINTEXT"
)



class WorkerState:
    def __init__(self):
        self.reset_state()

    def reset_state(self):
        self.worker_id = None
        self.update_tss = []
        self.stream_end_ts = None

        self.last_power_means = None
        self.last_power_M2s = None
        self.last_n_scans = 0

        self.log = deque(maxlen=8)
        self.stream_active = False


    def begin_stream(self, results):
        print("WorkerState begin_stream")
        assert self.stream_active == False
        self.reset_state()
        self.stream_active = True


    def end_stream(self, results):
        print("WorkerState end_stream")
        self.stream_active = False
        self.stream_end_ts = time.time() # time the worker state is declared finished, not producer_end_ts


    def update(self, results):
        self.update_tss.append(time.time())
        # results is a dictionary containing: n_averaged_scans, power_means, power_M2s, producer_timestamps, waiting_times, processing_times
        self.last_power_means = np.asarray(results["power_means"])
        self.last_power_M2s = np.asarray(results["power_M2s"])
        self.last_n_scans = results["n_averaged_scans"]

        self.log.appendleft(f"{time.strftime('%H:%M:%S')} | {self.last_n_scans} scans, ") # TODO: add net latency

worker_states = {}



class BenchmarkLogger:
    def __init__(self):
        self.started = False

        self.throughput_MB = None
        self.n_scans_per_batch = None
        self.n_partitions = None

        self.producer_begin_ts = None
        self.producer_end_ts = None

        self.analysis_end_ts = None

        self.records = []

    def get_filename(self, crashed):
        readable_ts = datetime.fromtimestamp(self.producer_begin_ts).strftime("%Y-%m-%d_%H-%M-%S")
        suffix = "--CRASHED.json" if crashed else ".json"
        return (
            f"benchmarks/"
            f"{readable_ts}--"
            f"{self.throughput_MB}MBps--"
            f"{self.n_scans_per_batch}SpB--"
            f"{self.n_partitions}parts"
            f"{suffix}"
        )

    def start(self, results):
        print("BenchmarkLogger start")
        self.producer_begin_ts = results["producer_begin_ts"]
        self.throughput_MB = results["throughput_MB"]
        self.n_scans_per_batch = results["n_scans_per_batch"]
        self.n_partitions = results["n_partitions"]
        self.started = True

    def close(self, results):
        self.analysis_end_ts = time.time()
        if self.started and results is None:
            print("BenchmarkLogger close due to crash")
            filename = self.get_filename(crashed=True)
            self.producer_end_ts = -1
        elif self.started == False:
            print("BenchmarkLogger close without doing anything")
            return
        else:
            print("BenchmarkLogger close")
            filename = self.get_filename(crashed=False)
            self.producer_end_ts = results["producer_end_ts"]
            
        benchmark_output = {
            "throughput_MB": self.throughput_MB,
            "n_partitions": self.n_partitions,
            "n_scans_per_batch": self.n_scans_per_batch,
            "producer_begin_ts": self.producer_begin_ts,
            "producer_end_ts": self.producer_end_ts,
            "analysis_end_ts": self.analysis_end_ts,
            "records": self.records
        }
        with open(filename, "w") as f:
            json.dump(benchmark_output, f, indent=2)
        self.started = False

    def update(self, msg):
        if msg.headers:
            for key, _ in msg.headers:
                if key == "BEGIN_STREAM":
                    return
                elif key == "END_STREAM":
                    return
        results = msg.value["results"]
        # Network latency = Total time (producer-consumer) - Time spent in the network and VMs
        net_latencies_ms = [
            (msg.timestamp/1000 - t_start) * 1000
            for t_start in results["producer_timestamps"]
        ]
 
        self.records.append({
            "worker_id": msg.value["worker_id"],
            "receive_ts": msg.timestamp/1000,
            "n_producer_batches": results["n_averaged_scans"]//self.n_scans_per_batch,
            "production_tss": results["producer_timestamps"].copy(),
            "net_latencies_ms": net_latencies_ms,
            "processing_times_ms": [1000*t for t in results["processing_times"]],
            "waiting_times": results["waiting_times"].copy(),
        })
        
print("Initializing benchmark logger...")
benchmark_logger = BenchmarkLogger()



class GlobalState:
    def __init__(self):
        self.reset_state()


    def reset_state(self):
        self.producer_begin_ts = None
        self.producer_end_ts = None

        self.producer_throughput_MB = None
        self.n_scans_per_producer_batch = None

        self.frequencies = []
        self.power_means = []
        self.power_M2s = []
        self.power_stds = []
        self.n_averaged_scans = 0

        self.stream_active = False


    def begin_stream(self, results):
        if self.stream_active == False:
            print("GlobalState begin_stream")
            self.reset_state()
            self.stream_active = True
            self.producer_throughput_MB = results["throughput_MB"]
            self.n_scans_per_producer_batch = results["n_scans_per_batch"]
            self.producer_begin_ts = results["producer_begin_ts"]
            self.frequencies = results["frequencies"].copy()

            # Also reset worker_states and signal the benchmark logger
            global worker_states, benchmark_logger
            worker_states = {}
            benchmark_logger.start(results)
            dashboard.reset()
        else:
            assert self.producer_throughput_MB == results["throughput_MB"]
            assert self.n_scans_per_producer_batch == results["n_scans_per_batch"]
            assert self.frequencies == results["frequencies"]
            self.producer_begin_ts = min(self.producer_begin_ts, results["producer_begin_ts"])


    def end_stream(self, results):
        print("GlobalState end_stream")
        self.stream_active = False
        self.producer_end_ts = results["producer_end_ts"]


    def update(self, results):
        print("Global update ", results["n_averaged_scans"], " scans")
        # results is a dictionary containing: n_averaged_scans, power_means, power_M2s, producer_timestamps, waiting_times, processing_times
        batch_means = np.asarray(results["power_means"])
        batch_M2s = np.asarray(results["power_M2s"])
        batch_n_scans = results["n_averaged_scans"]
        if self.n_averaged_scans == 0:
            self.power_means = batch_means
            self.power_M2s = batch_M2s
            self.n_averaged_scans = batch_n_scans
        else:
            delta = batch_means - self.power_means
            total_scans = self.n_averaged_scans + batch_n_scans
            self.power_means += delta * batch_n_scans / total_scans
            self.power_M2s += batch_M2s + delta**2 * self.n_averaged_scans * batch_n_scans / total_scans
            self.n_averaged_scans = total_scans

        # Convert M2s to stds
        if self.n_averaged_scans > 1:
            self.power_stds = np.sqrt(self.power_M2s / (self.n_averaged_scans - 1))

print("Initializing global state...")
global_state = GlobalState()



def update_states(msg):
    worker_id = msg.value["worker_id"]
    if worker_id not in worker_states: # Create worker state if it doesn't exist
        worker_states[worker_id] = WorkerState()
    worker_state = worker_states[worker_id]
    
    # Handle begin and end of stream signals
    if msg.headers:
        for key, _ in msg.headers:
            if key == "BEGIN_STREAM":
                global_state.begin_stream(msg.value)
                worker_state.begin_stream(msg.value)
                benchmark_logger.start(msg.value)
                return
            elif key == "END_STREAM":
                worker_state.end_stream(msg.value)
                # Send signal to global_state only if all workers finished
                if all(not ws.stream_active for ws in worker_states.values()):
                    global_state.end_stream(msg.value)
                    benchmark_logger.close(msg.value)
                    dashboard.stop()
                return
            
    results = msg.value["results"]
    # Update worker and global states, logger
    worker_state.update(results)
    global_state.update(results)
    benchmark_logger.update(msg)



# =============================================================================
# Dashboard
# =============================================================================
class Dashboard:
    def __init__(self):
        self.freeze_updates = True
        # Enable matplotlib interactive mode for real-time plotting
        plt.ion()
        self.fig = plt.figure(figsize=(15, 7))

        self.rebuild()
        self.render()

    class WorkerPlot:
        def __init__(self, axis, data, log_text):
            self.axis = axis
            self.data = data
            self.log_text = log_text

    def rebuild(self):
        print("Rebuilding dashboard")
        self.fig.clf()
        n_workers = max(len(worker_states), 1)
        # Layout: 1 top row for global, 1 middle row for spectra, 1 bottom row for text logs
        grid = self.fig.add_gridspec(3, n_workers, height_ratios=[2.0, 0.9, 1.2], hspace=0.35)

        self.global_axis = self.fig.add_subplot(grid[0, :])
        self.global_data, = self.global_axis.plot([], [], lw=2)
        self.global_errors = self.global_axis.errorbar([], [], yerr=[], fmt="none", capsize=2, alpha=0.7 )
        self.global_axis.set_xlim(FREQ_MIN_HZ, FREQ_MAX_HZ)
        self.global_axis.set_ylim(POWER_MIN, POWER_MAX)
        self.global_axis.set_yscale("log")
        self.global_axis.set_xlabel("Frequency (Hz)")
        self.global_axis.set_ylabel("Power")
        self.global_axis.grid(True)

        self.worker_plots = {}
        for column, worker_id in enumerate(sorted(worker_states)):
            # Setup individual worker spectrum plot
            spectrum_axis = self.fig.add_subplot(grid[1, column])
            spectrum_data, = spectrum_axis.plot([], [], lw=1.5)
            spectrum_axis.set_xlim(FREQ_MIN_HZ, FREQ_MAX_HZ)
            spectrum_axis.set_ylim(POWER_MIN, POWER_MAX)
            spectrum_axis.set_yscale("log")
            spectrum_axis.set_xlabel("Frequency (Hz)")
            spectrum_axis.set_ylabel("Power")
            spectrum_axis.grid(True)

            # Setup text box for worker logs
            log_axis = self.fig.add_subplot(grid[2, column])
            log_axis.axis("off")
            log_text = log_axis.text(0, 1, "", transform=log_axis.transAxes, va="top", fontsize=8)

            self.worker_plots[worker_id] = self.WorkerPlot(spectrum_axis, spectrum_data, log_text)


    def update(self):
        if self.freeze_updates:
            self.render()
            return
        # Rebuild the figure if the number of workers changes
        if len(self.worker_plots) != len(worker_states):
            self.rebuild()

        # Draw the latest data on the global plot
        if global_state.n_averaged_scans != 0:
            nonzero_power_means = np.maximum(global_state.power_means, 1e-12)
            self.global_data.set_data(global_state.frequencies, nonzero_power_means)
            self.global_errors.remove()
            self.global_errors = self.global_axis.errorbar(global_state.frequencies, nonzero_power_means, yerr=global_state.power_stds, fmt="none", capsize=2, alpha=0.7)
            #if global_state.producer_begin_ts is not None:
            self.global_axis.set_title(f"Cumulative Mean Spectrum ({global_state.n_averaged_scans} scans, elapsed {time.time() - global_state.producer_begin_ts:.1f} s)")

        # Iterate over all workers to update their subplots and text logs
        for worker_id in sorted(worker_states):
            worker_state = worker_states[worker_id]
            if worker_state.last_n_scans != 0:
                worker_plot = self.worker_plots[worker_id]

                nonzero_power_mean = np.maximum(worker_state.last_power_means, 1e-12)
                worker_plot.data.set_data(global_state.frequencies, nonzero_power_mean)
                worker_plot.axis.set_title(f"Worker {worker_id}")
                worker_plot.log_text.set_text(
                    f"N. of scans: {worker_state.last_n_scans}\n"
                    f"Age: {time.time() - worker_state.update_tss[-1]:.1f} s\n"
                    f"Avg age: {np.mean(np.diff(worker_state.update_tss)) if len(worker_state.update_tss) > 1 else 0:.1f} s\n"
                    f"\n"
                    f"Log\n" +
                    "-" * 30 + "\n" +
                    "\n".join(worker_state.log)
                )
        # Render the updated graphics
        self.render()

    def render(self):
        self.fig.canvas.draw_idle()
        self.fig.canvas.flush_events()

    def stop(self):
        self.freeze_updates = True

    def reset(self):
        self.freeze_updates = False


print("Initializing dashboard...")
dashboard = Dashboard()




try:
    # Keep running as long as the matplotlib window is open
    while plt.fignum_exists(dashboard.fig.number):
        # Fetch new messages (max 100ms wait)
        records = consumer.poll(timeout_ms=100)
        for _, messages in records.items():
            for msg in messages:
                update_states(msg)
                benchmark_logger.update(msg)
        dashboard.update()
finally:
    print("Closing...")
    benchmark_logger.close(None)
    consumer.close()