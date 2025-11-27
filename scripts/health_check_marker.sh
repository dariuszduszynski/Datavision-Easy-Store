#!/bin/bash
# Health check for the DES marker worker.

set -e

METRICS_PORT=${DES_METRICS_PORT:-9101}
HEALTH_ENDPOINT="http://localhost:${METRICS_PORT}/health"

if curl -f -s "${HEALTH_ENDPOINT}" > /dev/null 2>&1; then
    echo "Marker is healthy"
    exit 0
else
    echo "Marker is unhealthy"
    exit 1
fi
