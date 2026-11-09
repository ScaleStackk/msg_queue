# Lightweight Kafka broker for Legal GraphRAG

This directory defines a single-node Apache Kafka 4.4 broker running in KRaft
mode. It requires no ZooKeeper. On every start, it idempotently creates the
topics declared in `topics.conf` and preserves broker data in a Docker volume or
an EBS-backed bind mount.

The base is Apache's `apache/kafka-native:4.4.0` image. It has a smaller runtime
than the JVM image, but Apache marks the native image experimental and suitable
for development/testing rather than production. That is appropriate for the
Phase 1 demonstration. A production deployment should use the official JVM
image or a managed Kafka service.

## Topics

| Topic | Partitions | Purpose |
|---|---:|---|
| `judgment.uploaded` | 8 | Upload events consumed independently by KG and embedding groups |
| `kg.extracted` | 8 | S3 references to validated graph artifacts |
| `embeddings.created` | 8 | S3 references to embedding artifacts |
| `processing.status` | 3 | Compacted latest job status events |
| `processing.failed` | 3 | Dead-letter and audit events retained for 14 days |

The replication factor is `1` because Phase 1 uses one broker. Kafka is durable
across container restarts but remains a single point of failure.

## Build and run locally

Building downloads the official Kafka base image and Python topic-admin package:

```bash
docker compose build
docker compose up -d
docker compose logs -f kafka
```

Clients on the same host connect to `localhost:9092`. Containers in the Compose
network use `kafka:19092`.

## Run on EC2 with EBS

Mount an EBS filesystem at `/mnt/kafka`, set ownership for the image user, and
replace the named volume with a bind mount:

```yaml
volumes:
  - /mnt/kafka/data:/var/lib/kafka/data
```

Set `KAFKA_ADVERTISED_HOST` to the Kafka EC2 instance's private IP or private DNS
name before starting the broker:

```bash
cp .env.example .env
# Edit .env: KAFKA_ADVERTISED_HOST=kafka.internal
docker compose up -d
```

Allow TCP `9092` only from the web, worker, and index-writer security groups.
Do not expose this plaintext demonstration broker to the public internet.

## Customize topics

Edit `topics.conf` using this format:

```text
name|partitions|key=value,key=value
```

Topic creation is idempotent. Existing topics are retained; changing a line does
not destructively recreate an existing topic. Partition/config migrations should
be applied explicitly with Kafka administration tooling.

## Verify

```bash
docker compose ps
docker compose logs kafka
```

The startup log must contain `Configured topics:` followed by all five project
topics. Producers and consumers should use JSON messages containing document IDs
and S3 references, not PDFs, graph JSON, or embedding arrays.
