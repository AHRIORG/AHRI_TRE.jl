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

mount_share() {
    local share="$1"
    local mountpoint_path="$2"

    mkdir -p "${mountpoint_path}"

    if mountpoint -q "${mountpoint_path}"; then
        echo "Samba share already mounted at ${mountpoint_path}"
        return 0
    fi

    echo "Mounting Samba share ${share} at ${mountpoint_path}..."
    if mount -t cifs "${share}" "${mountpoint_path}" \
        -o username="${SAMBA_USERNAME}",password="${SAMBA_PASSWORD}",uid=0,gid=0; then
        echo "✓ Samba share mounted successfully at ${mountpoint_path}"
    else
        echo "✗ Failed to mount Samba share ${share}"
        return 1
    fi
}

# Mount the main lakehouse share
mount_share //dbn-pure-nas-01.ahri.org/lakehouse /mnt/lakehouse

# Mount the test lake share
mount_share //DBN-Pure-Nas-01.ahri.org/testlake /mnt/test_lake
