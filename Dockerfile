# syntax=docker/dockerfile:1.7
FROM apache/kafka-native:4.4.0

USER root

# The native broker image does not include kafka-topics.sh. Add a small Python
# admin client so the container can create an explicit, versioned topic set.
RUN apk add --no-cache python3 py3-pip \
    && python3 -m venv /opt/topic-init \
    && /opt/topic-init/bin/pip install --no-cache-dir kafka-python==3.0.12

COPY --chown=appuser:root entrypoint.sh init_topics.py topics.conf /opt/legal-kafka/
RUN chmod 0555 /opt/legal-kafka/entrypoint.sh /opt/legal-kafka/init_topics.py \
    && chmod 0444 /opt/legal-kafka/topics.conf \
    && mkdir -p /var/lib/kafka/data \
    && chown -R appuser:root /var/lib/kafka

USER appuser

VOLUME ["/var/lib/kafka/data"]
EXPOSE 9092

HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=5 \
  CMD ["bash", "-ec", "</dev/tcp/127.0.0.1/19092"]

ENTRYPOINT ["/opt/legal-kafka/entrypoint.sh"]

