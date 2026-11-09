#!/usr/bin/env python3
"""Wait for Kafka and idempotently create the configured project topics."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from kafka import KafkaAdminClient
from kafka.admin import NewTopic


def read_topics(path: Path) -> list[NewTopic]:
    topics: list[NewTopic] = []
    for line_number, raw_line in enumerate(path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        fields = [field.strip() for field in line.split("|", maxsplit=2)]
        if len(fields) < 2:
            raise ValueError(f"{path}:{line_number}: expected name|partitions|configs")

        name, partitions_text = fields[:2]
        if not name:
            raise ValueError(f"{path}:{line_number}: topic name is empty")
        partitions = int(partitions_text)
        if partitions < 1:
            raise ValueError(f"{path}:{line_number}: partitions must be positive")

        configs: dict[str, str] = {}
        if len(fields) == 3 and fields[2]:
            for item in fields[2].split(","):
                key, separator, value = item.partition("=")
                if not separator or not key.strip() or not value.strip():
                    raise ValueError(
                        f"{path}:{line_number}: invalid topic config {item!r}"
                    )
                configs[key.strip()] = value.strip()

        topics.append(
            NewTopic(
                name=name,
                num_partitions=partitions,
                replication_factor=1,
                topic_configs=configs,
            )
        )
    return topics


def connect(bootstrap_server: str, timeout_seconds: int) -> KafkaAdminClient:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return KafkaAdminClient(
                bootstrap_servers=bootstrap_server,
                client_id="legal-kafka-topic-initializer",
                request_timeout_ms=10_000,
                api_version_auto_timeout_ms=5_000,
            )
        except Exception as exc:  # Kafka may still be electing its controller.
            last_error = exc
            print(f"Waiting for Kafka at {bootstrap_server}: {exc}", flush=True)
            time.sleep(2)
    raise TimeoutError(
        f"Kafka did not become ready within {timeout_seconds}s: {last_error}"
    )


def main() -> int:
    bootstrap_server = os.environ.get("TOPIC_BOOTSTRAP_SERVER", "127.0.0.1:19092")
    topics_file = Path(
        os.environ.get("KAFKA_TOPICS_FILE", "/opt/legal-kafka/topics.conf")
    )
    timeout_seconds = int(os.environ.get("TOPIC_INIT_TIMEOUT_SECONDS", "120"))
    desired_topics = read_topics(topics_file)
    if not desired_topics:
        raise RuntimeError(f"No topics configured in {topics_file}")

    admin = connect(bootstrap_server, timeout_seconds)
    try:
        existing = set(admin.list_topics())
        missing = [topic for topic in desired_topics if topic.name not in existing]
        if missing:
            admin.create_topics(new_topics=missing, validate_only=False)
            print(
                "Created topics: " + ", ".join(topic.name for topic in missing),
                flush=True,
            )
        else:
            print("All configured topics already exist", flush=True)

        final_topics = set(admin.list_topics())
        absent = [topic.name for topic in desired_topics if topic.name not in final_topics]
        if absent:
            raise RuntimeError("Topics still missing after initialization: " + ", ".join(absent))

        print(
            "Configured topics: " + ", ".join(topic.name for topic in desired_topics),
            flush=True,
        )
    finally:
        admin.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Topic initialization error: {exc}", file=sys.stderr, flush=True)
        raise

