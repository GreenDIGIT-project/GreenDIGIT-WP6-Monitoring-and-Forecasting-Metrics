#!/bin/bash
BACKUP_DIR="/media/usb_influx_backup/Containers_backup"

# 1. Clear previous backups on the USB
echo "Cleaning old backups on USB..."
rm -rf $BACKUP_DIR/*.tar.gz

# 2. Backup all containers
docker ps --format "{{.ID}} {{.Names}}" | while read ID NAME; do
    # Convert name to lowercase
    LOWER_NAME=$(echo "$NAME" | tr '[:upper:]' '[:lower:]')
    IMAGE_NAME="latest_backup_$LOWER_NAME"
    
    echo "Processing $NAME..."

    # Delete the PREVIOUS local image to make room for the new one
    docker rmi "$IMAGE_NAME" 2>/dev/null
    
    # Create the NEW local image from the running container
    docker commit $ID "$IMAGE_NAME"
    
    # Save the NEW image to the USB
    echo "Saving to USB..."
    docker save "$IMAGE_NAME" | gzip > "$BACKUP_DIR/${LOWER_NAME}_latest.tar.gz"
done

echo "Done! Local images kept with 'latest_backup_' prefix."
