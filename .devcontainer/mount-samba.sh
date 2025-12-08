#!/bin/bash
set -e

# Load environment variables from .env file
if [ -f "/workspace/.env" ]; then
    # Remove carriage returns and spaces around = signs, then source
    source <(sed 's/\r$//' /workspace/.env | sed 's/ *= */=/g' | grep -v '^#' | grep -v '^$')
else
    echo "Warning: .env file not found. Skipping Samba mount."
    exit 0
fi

# Check if Samba credentials are provided
if [ -z "${SAMBA_USERNAME}" ] || [ -z "${SAMBA_PASSWORD}" ]; then
    echo "Samba credentials not found in .env. Skipping mount."
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
echo "Mounting Samba share..."
if mount -t cifs //dbn-pure-nas-01.ahri.org/lakehouse /mnt/lakehouse \
    -o username="${SAMBA_USERNAME}",password="${SAMBA_PASSWORD}",uid=0,gid=0; then
    echo "✓ Samba share mounted successfully at /mnt/lakehouse"
else
    echo "✗ Failed to mount Samba share"
    exit 1
fi
