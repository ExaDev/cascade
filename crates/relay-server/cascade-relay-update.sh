#!/bin/sh
# Replace the running cascade-relay with a new binary without refusing a
# connection: start the new version beside the old one, health-gate it, then
# drain the old one. If the new one fails its gate, stop it and leave the old
# one serving.
#
# Usage: cascade-relay-update.sh [--user] [--health-timeout SECONDS]
#                                [--settle SECONDS] NEW_BINARY
#
#   --user            Drive the per-user systemd manager (systemctl --user).
#   --health-timeout  How long the new slot has to report healthy. Default 30.
#   --settle          How long the new slot must then stay active and healthy
#                     before the old one is stopped. Default 5.
#
# Environment:
#   CASCADE_RELAY_LIB_DIR  Directory holding one subdirectory per slot, each
#                          with its cascade-relay binary.
#                          Default: /usr/local/lib/cascade-relay
#   CASCADE_RELAY_ETC_DIR  Directory holding relay-<slot>.env. Default:
#                          /etc/cascade
#   CASCADE_RELAY_UNIT     Unit prefix. Default: cascade-relay-handoff
#
# The slots (blue and green) accept on one systemd-owned socket, so starting
# the new slot is the switch: it begins accepting as soon as it is up. The
# gate therefore catches a slot that cannot start, crashes or reports itself
# unhealthy; a connection the new slot accepted before it failed is reset and
# the peer reconnects, as it does for any relay restart. See
# docs/relay-deployment.md.
#
# Exit status: 0 updated, 1 the new slot failed its gate and was rolled back
# (the old slot is still serving), 2 usage or environment error.

set -eu

scope=""
health_timeout=30
settle=5

while [ "$#" -gt 0 ]; do
    case "$1" in
        --user) scope="--user"; shift ;;
        --health-timeout) health_timeout="$2"; shift 2 ;;
        --settle) settle="$2"; shift 2 ;;
        -h|--help) sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        --) shift; break ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *) break ;;
    esac
done

if [ "$#" -ne 1 ]; then
    echo "usage: $0 [--user] [--health-timeout S] [--settle S] NEW_BINARY" >&2
    exit 2
fi
new_binary="$1"
if [ ! -x "$new_binary" ]; then
    echo "ERROR: $new_binary is not an executable file" >&2
    exit 2
fi

lib_dir="${CASCADE_RELAY_LIB_DIR:-/usr/local/lib/cascade-relay}"
etc_dir="${CASCADE_RELAY_ETC_DIR:-/etc/cascade}"
unit="${CASCADE_RELAY_UNIT:-cascade-relay-handoff}"

# shellcheck disable=SC2086
ctl() { systemctl $scope "$@"; }
slot_active() { ctl is-active --quiet "$unit@$1.service"; }

# The metrics address a slot's /health listens on, read from its env file with
# grep so the shared secret file is never opened.
slot_health_url() {
    addr=$(grep -E '^CASCADE_RELAY_METRICS=' "$etc_dir/relay-$1.env" | tail -n 1 | cut -d= -f2-)
    if [ -z "$addr" ]; then
        echo "ERROR: CASCADE_RELAY_METRICS is not set in $etc_dir/relay-$1.env" >&2
        exit 2
    fi
    # A wildcard bind address is probed over loopback.
    addr=$(printf '%s' "$addr" | sed 's/^0\.0\.0\.0:/127.0.0.1:/; s/^\[::\]:/127.0.0.1:/')
    printf 'http://%s/health' "$addr"
}

slot_healthy() {
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$(slot_health_url "$1")" || true)" = "200" ]
}

if slot_active blue && slot_active green; then
    echo "ERROR: both slots are active; a previous update was interrupted." >&2
    echo "       Stop the one that should not serve, then run the update again." >&2
    exit 2
fi

if slot_active blue; then
    old=blue; new=green
elif slot_active green; then
    old=green; new=blue
else
    old=""; new=blue
fi

echo "update: ${old:-no slot} serving; starting $new with $new_binary"

install -d "$lib_dir/$new"
# Install through a rename so a slot that is still draining on the same path
# keeps its mapped binary.
install -m 0755 "$new_binary" "$lib_dir/$new/cascade-relay.next"
mv -f "$lib_dir/$new/cascade-relay.next" "$lib_dir/$new/cascade-relay"

rollback() {
    echo "update: $1; stopping $new and leaving ${old:-nothing} serving" >&2
    ctl stop "$unit@$new.service" || true
    ctl reset-failed "$unit@$new.service" 2>/dev/null || true
    exit 1
}

ctl start --no-block "$unit@$new.service"

# Health gate: the new slot must report healthy within the timeout.
waited=0
until slot_healthy "$new"; do
    if ! slot_active "$new"; then
        rollback "$new exited before becoming healthy"
    fi
    if [ "$waited" -ge "$health_timeout" ]; then
        rollback "$new not healthy after ${health_timeout}s"
    fi
    sleep 1
    waited=$((waited + 1))
done

# Settle: it must then stay up and healthy, which catches a crash straight
# after startup.
settled=0
while [ "$settled" -lt "$settle" ]; do
    sleep 1
    settled=$((settled + 1))
    if ! slot_active "$new" || ! slot_healthy "$new"; then
        rollback "$new failed during its ${settle}s settle period"
    fi
done

if [ -n "$old" ]; then
    echo "update: $new healthy; draining $old"
    # Stop blocks until the old slot has drained and exited. It no longer
    # accepts, so new connections go to $new; open sessions finish first.
    ctl stop "$unit@$old.service"
fi
echo "update: done; $new is serving"
