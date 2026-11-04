#!/bin/bash

# Define the Raspberry Pi IPs in an array (node01 through node12)
nodes=(
  "10.64.44.70" "10.64.44.71" "10.64.44.72" "10.64.44.73"
  "10.64.44.74" "10.64.44.75" "10.64.44.190" "10.64.44.191"
  "10.64.44.192" "10.64.44.193" "10.64.44.194" "10.64.44.195"
)

echo "--- Node Status Report ---"

# Loop through the array
for i in "${!nodes[@]}"; do
  # Format the node name (node01, node02, etc.)
  node_name=$(printf "node%02d" $((i + 1)))
  ip="${nodes[$i]}"

  # Ping the IP (-c 1: one packet, -W 1: one second timeout)
  if ping -c 1 -W 1 "$ip" > /dev/null 2>&1; then
    status="online"
  else
    status="OFFLINE"
  fi

  # Print each node on a new line
  echo "$node_name=$status"
done

echo "--------------------------"
