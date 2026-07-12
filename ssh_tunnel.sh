#!/bin/bash

# ================================================================ #
#                    SSH Tunnels Configuration                     #
# ================================================================ #

# Before running the script, start Jupyter Notebook on the scheduler node and save the token/URL
# ssh -J dghezzi@gate.cloudveneto.it -i private/mapd_group_keypair.pem debian@10.67.22.102
# source pyvenv/bin/activate
# jupyter notebook --port=8888

USERNAME="dghezzi"
KEY_PATH="mapd_group_keypair.pem"

GATE_HOST="gate.cloudveneto.it"
NODE_IP="10.67.22.102"
PORT_JUPYTER="8888"
PORT_DASK="8797"
PORT_KAFKA="9092"

echo "Starting SSH tunnel for Jupyter Notebook & Dask Dashboard..."
echo "Destination : debian@$NODE_IP"
echo "Ports       : Jupyter ($PORT_JUPYTER) | Dask ($PORT_DASK) | Kafka ($PORT_KAFKA)"
echo "Jump host   : $USERNAME@$GATE_HOST"
echo "Using key   : $KEY_PATH"

echo "To access Jupyter Notebook: http://localhost:${PORT_JUPYTER}"
echo "To access Dask Dashboard: http://localhost:${PORT_DASK}/status"
echo "Kafka port: ${PORT_KAFKA}"

# Open an SSH tunnel for Jupyter, Dask, and Kafka (with an heartbeat to keep the connection alive every 60 seconds)
ssh -o ServerAliveInterval=60 -i "$KEY_PATH" -N -L ${PORT_JUPYTER}:localhost:${PORT_JUPYTER} -L ${PORT_DASK}:localhost:${PORT_DASK} -L ${PORT_KAFKA}:localhost:${PORT_KAFKA} -J ${USERNAME}@${GATE_HOST} debian@${NODE_IP}

# REMEMBER:
# The producer.py script has to run after the tunnel is established, otherwise it will not be able to connect to Kafka.
# Moreover, it has to be run in the a different terminal, otherwise the tunnel will be closed when the script ends.