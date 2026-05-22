FROM bitnami/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# ── Iceberg Spark Runtime (inclui suporte ao REST catalog) ──────
ARG ICEBERG_VERSION=1.5.2
RUN curl -fsSL -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"

# ── AWS SDK v2 bundle para S3FileIO do Iceberg ──────────────────
ARG ICEBERG_AWS_VERSION=1.5.2
RUN curl -fsSL -o /opt/bitnami/spark/jars/iceberg-aws-bundle-${ICEBERG_AWS_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_AWS_VERSION}/iceberg-aws-bundle-${ICEBERG_AWS_VERSION}.jar"

USER 1001
