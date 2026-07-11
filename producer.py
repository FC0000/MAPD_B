from kafka import KafkaProducer

print("Initializing kafka producer")

KAFKA_TOPIC = "topic_stream"
BOOTSTRAP_SERVERS = "localhost:9092"

# All settings explained in https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    #client_id                  # Appears in server-side logs. Default: ‘kafka-python-producer-#’ (appended with a unique number per instance)
    key_serializer = None,      # Send raw bytes
    value_serializer = None,    # Send raw bytes
    #transactional_id           # not needed
    enable_idempotence = True,  # Ensure that exactly one copy of each message is written in the stream
    #delivery_timeout_ms        # not needed
    #acks = ,                   # use default all
    compression_type=None,      # No compression
    #retries                    # not needed
    batch_size = 0,             # Disable batching, we will do our own
    #linger_ms                  # not needed, batching is disabled
    #partitioner                # use default
    #connections_max_idle_ms    # use default
    max_block_ms = 3000,                        # Max block time if buffer is full
    max_request_size = 32*1024*1024,           # The maximum size of a request. This is also effectively a cap on the maximum record size. Note that the server has its own cap on record size which may be different from this
    max_in_flight_requests_per_connection = 1, # Requests are pipelined to kafka brokers up to this number of maximum requests per broker connection
    security_protocol = "PLAINTEXT",    
    )

producer.partitions_for(KAFKA_TOPIC) # warm up the producer

import glob
import sys
import time
import os
import datetime

i_files = sorted(glob.glob("data/duck_i_*.dat"))
q_files = sorted(glob.glob("data/duck_q_*.dat"))
assert len(i_files) == len(q_files) == 31
files_count = len(i_files)

FILE_SIZE = os.path.getsize(i_files[0])
assert FILE_SIZE == 33554432
HALF_TOTAL_SIZE = FILE_SIZE * files_count

print("Loading files...\n")

i_data = bytearray(HALF_TOTAL_SIZE)
q_data = bytearray(HALF_TOTAL_SIZE)

for idx, (i_file, q_file) in enumerate(zip(i_files, q_files)):
    sys.stdout.write(f"\rLoading files {idx + 1}/{len(i_files)}")
    sys.stdout.flush()

    start = idx * FILE_SIZE
    end = start + FILE_SIZE

    with open(i_file, "rb") as f:
        n = f.readinto(memoryview(i_data)[start:end])
        assert n == FILE_SIZE

    with open(q_file, "rb") as f:
        n = f.readinto(memoryview(q_data)[start:end])
        assert n == FILE_SIZE
print("\n")

SCANS_PER_BATCH = 16
THROUGHPUT = 262144 # B/s

SAMPLES_PER_SCAN = 2048
SAMPLES_PER_BATCH = SCANS_PER_BATCH * SAMPLES_PER_SCAN
HALF_BATCH_SIZE = 4 * SAMPLES_PER_BATCH

DT = 2*HALF_BATCH_SIZE/THROUGHPUT

n_batches = HALF_TOTAL_SIZE // HALF_BATCH_SIZE

print(f"Streaming {n_batches} batches of size {2*HALF_BATCH_SIZE}B...")
start_time = time.perf_counter()
for i in range(n_batches):
    start = i * HALF_BATCH_SIZE
    end = start + HALF_BATCH_SIZE

    i_bytes = i_data[start:end]
    q_bytes = q_data[start:end]

    packet = i_bytes + q_bytes # concatenation
    
    timestamp = time.time()
    # TODO: for high rates, print less often
    print(f"[{datetime.datetime.fromtimestamp(timestamp)}] Sending batch {i+1}/{n_batches} (scans up to {(i+1)*SCANS_PER_BATCH})")
    producer.send(KAFKA_TOPIC, value=packet, headers=[('producer_ts', str(timestamp).encode('utf-8'))])
    producer.flush()
    

    target_time = start_time + i * DT
    sleep_time = target_time - time.perf_counter()
    if sleep_time > 0:
        time.sleep(sleep_time)
    else:
        print(f"missed send by {sleep_time}s")

#producer.flush()
producer.close()

print("\nDone streaming.")