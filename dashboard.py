import json
import time
from datetime import datetime
from collections import deque
import matplotlib.pyplot as plt
import numpy as np
from kafka import KafkaConsumer
import os

# =============================================================================
# Configuration
# =============================================================================
TOPIC = "topic_results"
BOOTSTRAP = "localhost:9092"

FREQ_MIN_HZ = -1.1e6
FREQ_MAX_HZ = 1.1e6
POWER_MIN = 1e-4
POWER_MAX = 30

N_SAMPLES_PER_SCAN = 2048  # Number of complex samples in each scan
SAMPLING_FREQ_HZ = 2e6     # ADC readout frequency
FREQUENCIES = np.fft.fftfreq(N_SAMPLES_PER_SCAN, d = 1 / SAMPLING_FREQ_HZ).tolist()


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


class BeginSignal:
    def __init__(self, msg):
        self.receive_ts = time.time()
        self.producer_begin_ts = msg.timestamp/1000
        self.throughput_MB = msg.value["throughput_MB"]
        self.n_scans_per_batch = msg.value["n_scans_per_batch"]
        self.n_partitions = msg.value["n_partitions"]
        self.total_n_scans = msg.value["total_n_scans"]

class EndSignal:
    def __init__(self, msg):
        self.receive_ts = time.time()
        self.producer_end_ts = msg.timestamp/1000

class FinishSignal:
    def __init__(self, n_total_scans):
        self.finish_ts = time.time()
        self.n_total_scans = n_total_scans

class UpdateMessage:
    def __init__(self, msg):
        self.receive_ts = time.time()
        self.result_ts = msg.timestamp/1000
        self.worker_id = msg.value["worker_id"]
        results = msg.value["results"]
        self.power_means = np.asarray(results["power_means"])
        self.power_M2s = np.asarray(results["power_M2s"])
        self.n_scans = results["n_scans"]
        self.producer_ts = results["producer_ts"]
        self.processing_time = results["processing_time"]

class WorkerState:
    def __init__(self):
        self.worker_id = None
        self.update_tss = deque(maxlen=20)

        self.last_power_means = None
        self.last_power_M2s = None
        self.last_n_scans = 0

        self.log = deque(maxlen=8)


    def update(self, update_msg: UpdateMessage):
        self.update_tss.append(update_msg.receive_ts)
        self.last_power_means = update_msg.power_means
        self.last_power_M2s = update_msg.power_M2s
        self.last_n_scans = update_msg.n_scans

        self.log.appendleft(f"{time.strftime('%H:%M:%S')} | {self.last_n_scans} scans, ...")

worker_states = {}



class BenchmarkLogger:
    def __init__(self):
        self.started = False
        self.finished = False
        self.received_end_signal = False

        self.throughput_MB = None
        self.n_scans_per_batch = None
        self.workers = []
        self.n_partitions = None

        self.producer_begin_ts = None
        self.producer_end_ts = None

        self.analysis_end_ts = None

        self.records = []


    def on_producer_begin_signal(self, begin_signal: BeginSignal):
        self.producer_begin_ts = begin_signal.producer_begin_ts
        self.throughput_MB = begin_signal.throughput_MB
        self.n_scans_per_batch = begin_signal.n_scans_per_batch
        self.n_partitions = begin_signal.n_partitions
        self.n_total_scans = begin_signal.total_n_scans
        self.workers = []
        self.started = True
        self.finished = False
        self.received_end_signal = False


    def on_producer_end_signal(self, end_signal):
        self.producer_end_ts = end_signal.producer_end_ts
        self.received_end_signal = True
        if self.finished == True: # if already finished, close here
            self.close()


    def on_finish(self, finish_signal: FinishSignal):
        self.analysis_end_ts = finish_signal.finish_ts
        self.finished = True
        if self.received_end_signal: # if did not receive end yet, delay closing
            self.close()


    def close(self):
        self.started = False
        benchmark_output = {
            "throughput_MB": self.throughput_MB,
            "n_workers": len(self.workers),
            "n_partitions": self.n_partitions,
            "n_total_scans": self.n_total_scans,
            "n_scans_per_batch": self.n_scans_per_batch,
            "producer_begin_ts": self.producer_begin_ts,
            "producer_end_ts": self.producer_end_ts,
            "analysis_end_ts": self.analysis_end_ts,
            "records": self.records
        }

        readable_ts = datetime.fromtimestamp(self.producer_begin_ts).strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"benchmarks/{readable_ts}--{self.throughput_MB}MBps--{self.n_scans_per_batch}SpB--{len(self.workers)}ws--{self.n_partitions}parts.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            json.dump(benchmark_output, f, indent=2)
        

    def update(self, update_msg: UpdateMessage):
        if not self.started:
            raise RuntimeError("BenchmarkLogger has not started")
        # Network latency = Total time (producer-consumer)
        batch_latency_ms = 1000 * (update_msg.receive_ts - update_msg.producer_ts)

        if update_msg.worker_id not in self.workers:
            self.workers.append(update_msg.worker_id)
 
        self.records.append({
            "worker_id": update_msg.worker_id,
            "result_ts": update_msg.result_ts,
            "receive_ts": update_msg.receive_ts,
            "n_scans": update_msg.n_scans,
            "production_ts": update_msg.producer_ts,
            "batch_latency_ms": batch_latency_ms,
            "processing_time_ms": 1000* update_msg.processing_time,
        })
        

benchmark_logger = BenchmarkLogger()



class GlobalState:
    def __init__(self):
        self.reset_state()


    def reset_state(self):
        self.producer_begin_ts = None
        self.producer_end_ts = None

        self.producer_throughput_MB = None
        self.n_scans_per_producer_batch = None

        self.power_means = []
        self.power_M2s = []
        self.power_stds = []
        self.n_averaged_scans = 0
        self.total_n_scans = None

        self.stream_active = False


    def on_producer_begin_signal(self, begin_signal: BeginSignal):
        self.reset_state()
        self.stream_active = True
        self.producer_throughput_MB = begin_signal.throughput_MB
        self.n_scans_per_producer_batch = begin_signal.n_scans_per_batch
        self.producer_begin_ts = begin_signal.producer_begin_ts
        self.total_n_scans = begin_signal.total_n_scans


    def on_producer_end_signal(self, end_signal: EndSignal):
        self.producer_end_ts = end_signal.producer_end_ts


    def update(self, update_msg: UpdateMessage):
        if not self.stream_active:
            raise RuntimeError("GlobalState has not started")

        batch_means = update_msg.power_means
        batch_M2s = update_msg.power_M2s
        batch_n_scans = update_msg.n_scans
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

        if self.n_averaged_scans == self.total_n_scans:
            finish_signal = FinishSignal(self.total_n_scans)
            self.stream_active = False
            benchmark_logger.on_finish(finish_signal)
            dashboard.on_finish(finish_signal)

global_state = GlobalState()


# =============================================================================
# Dashboard
# =============================================================================
class Dashboard:
    def __init__(self):
        self.freeze_updates = True
        # Enable matplotlib interactive mode for real-time plotting
        plt.ion()
        self.fig = plt.figure(figsize=(10, 5))

        self.rebuild()
        self.render()

    class WorkerPlot:
        def __init__(self, axis, data, log_text):
            self.axis = axis
            self.data = data
            self.log_text = log_text

    def rebuild(self):
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
            self.global_data.set_data(FREQUENCIES, nonzero_power_means)
            self.global_errors.remove()
            self.global_errors = self.global_axis.errorbar(FREQUENCIES, nonzero_power_means, yerr=global_state.power_stds, fmt="none", capsize=2, alpha=0.7)
            #if global_state.producer_begin_ts is not None:
            self.global_axis.set_title(f"Cumulative Mean Spectrum ({global_state.n_averaged_scans} scans, elapsed {time.time() - global_state.producer_begin_ts:.1f} s)")

        # Iterate over all workers to update their subplots and text logs
        for worker_id in sorted(worker_states):
            worker_state = worker_states[worker_id]
            if worker_state.last_n_scans != 0:
                worker_plot = self.worker_plots[worker_id]

                nonzero_power_mean = np.maximum(worker_state.last_power_means, 1e-12)
                worker_plot.data.set_data(FREQUENCIES, nonzero_power_mean)
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

    def on_producer_begin_signal(self, begin_signal: BeginSignal):
        self.freeze_updates = False

    def on_producer_end_signal(self, end_signal: EndSignal):
        pass

    def on_finish(self, finish_signal: FinishSignal):
        # refresh gui before freezing
        self.update()
        self.freeze_updates = True

dashboard = Dashboard()


def handle_message(msg):
    # Handle begin and end of stream signals
    if msg.headers:
        for key, _ in msg.headers:
            if key == "BEGIN_STREAM":
                begin_signal = BeginSignal(msg)

                global_state.on_producer_begin_signal(begin_signal)
                global worker_states
                worker_states = {}
                benchmark_logger.on_producer_begin_signal(begin_signal)
                dashboard.on_producer_begin_signal(begin_signal)
                return
            elif key == "END_STREAM":
                end_signal = EndSignal(msg)

                global_state.on_producer_end_signal(end_signal)
                benchmark_logger.on_producer_end_signal(end_signal)
                dashboard.on_producer_end_signal(end_signal)
                return
    
    update_msg = UpdateMessage(msg)
    if update_msg.worker_id not in worker_states: # Create worker state if it doesn't exist
        worker_states[update_msg.worker_id] = WorkerState()
    worker_state = worker_states[update_msg.worker_id]
            
    # Update worker and global states, logger
    worker_state.update(update_msg)
    benchmark_logger.update(update_msg)
    global_state.update(update_msg) # must be last one to be updated since it will call on_finish


try:
    # Keep running as long as the matplotlib window is open
    while plt.fignum_exists(dashboard.fig.number):
        # Fetch new messages (max 100ms wait)
        records = consumer.poll(timeout_ms=100)
        for _, messages in records.items():
            for msg in messages:
                handle_message(msg)
        dashboard.update()
finally:
    print("Closing...")
    try:
        consumer.close()
    except:
        pass