version: "3.9"

networks:
  ${HUDI_NETWORK}:
    name: ${HUDI_NETWORK}

services:
  ${CONTAINER_UID}hudi-minio:
    image: minio/minio:RELEASE.2024-03-15T01-07-19Z
    container_name: ${CONTAINER_UID}hudi-minio
    command: server /data --console-address ":${MINIO_CONSOLE_PORT}"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    ports:
      - "${MINIO_API_PORT}:9000"
      - "${MINIO_CONSOLE_PORT}:9001"
    networks:
      - ${HUDI_NETWORK}

  ${CONTAINER_UID}hudi-minio-mc:
    image: minio/mc:RELEASE.2024-03-15T07-10-57Z
    container_name: ${CONTAINER_UID}hudi-minio-mc
    entrypoint: |
      /bin/bash -c "
      set -euo pipefail
      sleep 5
      mc alias set myminio http://${CONTAINER_UID}hudi-minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
      mc mb --quiet myminio/${HUDI_BUCKET} || true
      mc mb --quiet myminio/${HUDI_BUCKET}-tmp || true
      "
    depends_on:
      - ${CONTAINER_UID}hudi-minio
    networks:
      - ${HUDI_NETWORK}

  ${CONTAINER_UID}hudi-metastore-db:
    image: postgres:14
    container_name: ${CONTAINER_UID}hudi-metastore-db
    environment:
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hive
      POSTGRES_DB: metastore
    networks:
      - ${HUDI_NETWORK}

  ${CONTAINER_UID}hudi-metastore:
    image: starburstdata/hive:3.1.2-e.18
    container_name: ${CONTAINER_UID}hudi-metastore
    hostname: ${CONTAINER_UID}hudi-metastore
    environment:
      HIVE_METASTORE_DRIVER: org.postgresql.Driver
      HIVE_METASTORE_JDBC_URL: jdbc:postgresql://${CONTAINER_UID}hudi-metastore-db:5432/metastore
      HIVE_METASTORE_USER: hive
      HIVE_METASTORE_PASSWORD: hive
      HIVE_METASTORE_WAREHOUSE_DIR: s3a://${HUDI_BUCKET}/warehouse
      S3_ENDPOINT: http://${CONTAINER_UID}hudi-minio:9000
      S3_ACCESS_KEY: ${MINIO_ROOT_USER}
      S3_SECRET_KEY: ${MINIO_ROOT_PASSWORD}
      S3_PATH_STYLE_ACCESS: "true"
      REGION: "us-east-1"
    depends_on:
      - ${CONTAINER_UID}hudi-metastore-db
      - ${CONTAINER_UID}hudi-minio
    ports:
      - "${HIVE_METASTORE_PORT}:9083"
    networks:
      - ${HUDI_NETWORK}

  ${CONTAINER_UID}hudi-spark:
    image: bitnami/spark:3.4.1
    container_name: ${CONTAINER_UID}hudi-spark
    hostname: ${CONTAINER_UID}hudi-spark
    environment:
      HUDI_BUNDLE_VERSION: ${HUDI_BUNDLE_VERSION}
      HUDI_BUNDLE_URL: ${HUDI_BUNDLE_URL}
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      HUDI_BUCKET: ${HUDI_BUCKET}
      HIVE_METASTORE_URIS: thrift://${CONTAINER_UID}hudi-metastore:9083
      S3_ENDPOINT: http://${CONTAINER_UID}hudi-minio:9000
    volumes:
      - ./scripts:/opt/hudi-scripts
      - ./cache:/opt/hudi-cache
    depends_on:
      - ${CONTAINER_UID}hudi-minio
      - ${CONTAINER_UID}hudi-minio-mc
      - ${CONTAINER_UID}hudi-metastore
    command: ["/opt/hudi-scripts/init.sh"]
    ports:
      - "${SPARK_UI_PORT}:8080"
    networks:
      - ${HUDI_NETWORK}
