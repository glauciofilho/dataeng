#!/bin/sh
set -e

# Wait for MinIO to be ready
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"; do
  echo "Waiting for MinIO..."
  sleep 2
done

# Create buckets
for bucket in bronze silver gold warehouse; do
  mc mb --ignore-existing local/$bucket
  echo "Bucket '$bucket' ready."
done

echo "All MinIO buckets created."
