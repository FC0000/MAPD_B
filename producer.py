from kafka import KafkaProducer
import glob
import sys
import time
import os
import datetime

print("Initializing kafka producer")

# =============================================================================
# Configuration
# =============================================================================
KAFKA_TOPIC = "topic_stream"
BOOTSTRAP_SERVERS = "localhost:9092"

'''
# to see Kafka debug logs
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("kafka").setLevel(logging.DEBUG)
'''

# =============================================================================
# Kafka Producer Setup
# =============================================================================
# All settings explained in https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    #client_id                  # Appears in server-side logs. Default: ‘kafka-python-producer-#’ (appended with a unique number per instance)
    key_serializer = None,      # Send raw bytes
    value_serializer = None,    # Send raw bytes
    #transactional_id           # Not needed
    enable_idempotence = True,# Ensure that exactly one copy of each message is written in the stream (Commented out for kafka-python-ng compatibility)
    #delivery_timeout_ms        # Not needed
    #acks = 0,                  # Use default all
    compression_type=None,      # No compression
    #retries                    # Not needed
    batch_size = 0,             # Disable batching, we will do our own
    #linger_ms                  # Not needed, batching is disabled
    #partitioner                # Use default
    #connections_max_idle_ms    # Use default
    max_block_ms = 3000,                        # Max block time if buffer is full
    max_request_size = 64*1024*1024,           # The maximum size of a request. This is also effectively a cap on the maximum record size. Note that the server has its own cap on record size which may be different from this
    max_in_flight_requests_per_connection = 1, # Requests are pipelined to kafka brokers up to this number of maximum requests per broker connection
    security_protocol = "PLAINTEXT",    
)

producer.partitions_for(KAFKA_TOPIC) # Warm up the producer to fetch cluster metadata

# =============================================================================
# Data Loading
# =============================================================================
# Locate and pair the I and Q data files
i_files = sorted(glob.glob("data/duck_i_*.dat"))
q_files = sorted(glob.glob("data/duck_q_*.dat"))
assert len(i_files) == len(q_files) == 31
files_count = len(i_files)

# Ensure file size matches expectations
FILE_SIZE = os.path.getsize(i_files[0])
assert FILE_SIZE == 33554432
HALF_TOTAL_SIZE = FILE_SIZE * files_count

print("Loading files...\n")

# Pre-allocate large bytearrays to hold all I and Q data in memory
i_data = bytearray(HALF_TOTAL_SIZE)
q_data = bytearray(HALF_TOTAL_SIZE)

# Read files sequentially directly into the pre-allocated buffers
for idx, (i_file, q_file) in enumerate(zip(i_files, q_files)):
    sys.stdout.write(f"\rLoading files {idx + 1}/{len(i_files)}")
    sys.stdout.flush()

    start = idx * FILE_SIZE
    end = start + FILE_SIZE

    # Read I data
    with open(i_file, "rb") as f:
        n = f.readinto(memoryview(i_data)[start:end])
        assert n == FILE_SIZE

    # Read Q data
    with open(q_file, "rb") as f:
        n = f.readinto(memoryview(q_data)[start:end])
        assert n == FILE_SIZE
print("\n")

# =============================================================================
# Streaming Parameters
# =============================================================================
SCANS_PER_BATCH = 1024
TARGET_THROUGHPUT = 16*1024*1024 # Target throughput in Bytes/s (16384 = 16 kB/s, final target will be 16777216 for 16 MB/s)

SAMPLES_PER_SCAN = 2048
SAMPLES_PER_BATCH = SCANS_PER_BATCH * SAMPLES_PER_SCAN
HALF_BATCH_SIZE = 4 * SAMPLES_PER_BATCH # 4 bytes per float32 sample

# Time to wait between batch transmissions to maintain TARGET_THROUGHPUT
TARGET_PUBLISH_INTERVAL = 2 * HALF_BATCH_SIZE / TARGET_THROUGHPUT

n_batches = HALF_TOTAL_SIZE // HALF_BATCH_SIZE

print(f"Streaming {n_batches} batches of size {2*HALF_BATCH_SIZE}B...")

# =============================================================================
# Main Streaming Loop
# =============================================================================
try:
    start_time = time.perf_counter()
    
    for i in range(n_batches):
        # Calculate slicing indices for the current batch
        start = i * HALF_BATCH_SIZE
        end = start + HALF_BATCH_SIZE

        # Extract I and Q bytes for this batch
        i_bytes = i_data[start:end]
        q_bytes = q_data[start:end]

        packet = i_bytes + q_bytes # Concatenate I and Q into a single packet
        
        timestamp = time.time()
        print(f"[{datetime.datetime.fromtimestamp(timestamp)}] Sending batch {i+1}/{n_batches} (scans up to {(i+1)*SCANS_PER_BATCH})")

        # Send packet to Kafka and measure the time for sending
        t_send_start = time.perf_counter()
        future = producer.send(
            KAFKA_TOPIC, 
            value=packet, 
            headers=[('producer_ts', str(timestamp).encode('utf-8')), ('throughput', str(TARGET_THROUGHPUT).encode('utf-8')), ('scans_per_batch', str(SCANS_PER_BATCH).encode('utf-8'))] # Attach metadata
        )
        t_send_done = time.perf_counter()

        # Flush the producer to obtain the batch size we want, and measure the time for flushing
        t_flush_start = time.perf_counter()
        producer.flush()
        t_flush_done = time.perf_counter()

        print(f"  send()={t_send_done - t_send_start:.3f}s  flush()={t_flush_done - t_flush_start:.3f}s")
        
        # Calculate the target time and sleep if necessary to maintain the target publish interval
        target_time = start_time + (i + 1) * TARGET_PUBLISH_INTERVAL
        sleep_time = target_time - time.perf_counter()
        
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            # If processing and network I/O took longer than the interval, warn about falling behind
            print(f"missed send by {sleep_time}s")
            
finally:
    print("Closing producer...")
    producer.close()

print("\nDone streaming.")