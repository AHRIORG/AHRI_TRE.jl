#!/bin/bash
set -e

# Load environment variables from .env file
if [ -f /workspace/.env ]; then
    # Remove carriage returns, spaces around =, and load variables
    set -a
    source <(grep -v '^#' /workspace/.env | sed 's/\r$//' | sed 's/ *= */=/g')
    set +a
fi

# Check if credentials are provided
if [ -z "$SAMBA_USERNAME" ] || [ -z "$SAMBA_PASSWORD" ]; then
    echo "Warning: SAMBA_USERNAME or SAMBA_PASSWORD not found in .env file"
    echo "Samba share will not be mounted"
    exit 0
fi

# Create mount point
mkdir -p /mnt/lakehouse

# Check if already mounted
if mountpoint -q /mnt/lakehouse; then
    echo "Samba share already mounted at /mnt/lakehouse"
    exit 0
fi

# Mount the Samba share
echo "Mounting Samba share //dbn-pure-nas-01.ahri.org/lakehouse..."
mount -t cifs //dbn-pure-nas-01.ahri.org/lakehouse /mnt/lakehouse \
    -o username="$SAMBA_USERNAME",password="$SAMBA_PASSWORD",uid=0,gid=0

if [ $? -eq 0 ]; then
    echo "Successfully mounted Samba share at /mnt/lakehouse"
else
    echo "Failed to mount Samba share"
    exit 1
fi
