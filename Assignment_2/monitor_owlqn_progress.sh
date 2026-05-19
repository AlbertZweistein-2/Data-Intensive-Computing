#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <application_id> [interval_seconds]"
    exit 1
fi

APP_ID="$1"
shift
INTERVAL_SECONDS="${1:-600}"

echo "Monitoring OWLQN progress for $APP_ID every $INTERVAL_SECONDS seconds."
echo "Press Ctrl+C to stop."

while true; do
    TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"
    COUNT="$(yarn logs -applicationId "$APP_ID" 2>/dev/null | grep -c "OWLQN: Val and Grad Norm" || true)"
    echo "$TIMESTAMP $COUNT"
    sleep "$INTERVAL_SECONDS"
done
