#!/bin/bash
set -e

# Fix Docker socket permissions
if [ -S /var/run/docker.sock ]; then
    sudo chmod 666 /var/run/docker.sock
fi

# Execute the original entrypoint
exec /entrypoint "$@"


