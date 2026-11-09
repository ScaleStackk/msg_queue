#!/usr/bin/env bash
set -euo pipefail

broker_pid=""

shutdown() {
  if [[ -n "${broker_pid}" ]] && kill -0 "${broker_pid}" 2>/dev/null; then
    kill -TERM "${broker_pid}"
    wait "${broker_pid}" || true
  fi
}

trap shutdown TERM INT

/etc/kafka/docker/run &
broker_pid=$!

if ! /opt/topic-init/bin/python /opt/legal-kafka/init_topics.py; then
  echo "Topic initialization failed; stopping Kafka" >&2
  shutdown
  exit 1
fi

wait "${broker_pid}"

