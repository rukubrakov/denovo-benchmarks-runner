#!/bin/bash
# Stop Prefect server instance

INSTANCE_NAME="prefect-server"

if apptainer instance list | grep -q "$INSTANCE_NAME"; then
    echo "Stopping Prefect server..."
    apptainer instance stop "$INSTANCE_NAME"
    echo "✓ Server stopped"
else
    echo "Prefect server is not running"
fi
