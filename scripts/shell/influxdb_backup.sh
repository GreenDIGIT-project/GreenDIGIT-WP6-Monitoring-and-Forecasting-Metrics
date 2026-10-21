#!/bin/bash

# Variables
BACKUP_DIR="/media/usb_influx_backup/$(date +\%Y-\%m-\%d)"
DATABASE="shelly_power"
DATABASE2="workloads_IoT"

# Make sure backup directory exists
mkdir -p "$BACKUP_DIR"

# Run InfluxDB backup
#influx backup -t YOUR_TOKEN_HERE -b "$DATABASE" "$BACKUP_DIR"
influxd backup -database "$DATABASE" "$BACKUP_DIR"
influxd backup -database "$DATABASE2" "$BACKUP_DIR"

# Optional: Remove old backups (older than 7 days)
find /media/usb_influx_backup/ -mindepth 1 -maxdepth 1 -type d -mtime +7 -exec rm -rf {} \;
