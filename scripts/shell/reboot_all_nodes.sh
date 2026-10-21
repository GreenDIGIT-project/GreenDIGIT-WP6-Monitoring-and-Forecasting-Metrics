#!/bin/bash

PASSWORD="raspberry"
USER="pi"
NODES=("10.64.44.70" "10.64.44.71" "10.64.44.72" "10.64.44.73" "10.64.44.74" "10.64.44.75" "10.64.44.190" "10.64.44.191" "10.64.44.192" "10.64.44.193" "10.64.44.194" "10.64.44.195");

for IP in "${NODES[@]}"; do

    echo "Running command on $IP..."
    sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no $USER@$IP "sudo reboot"
done
