# save sqlite
docker exec iceberg-rest bash -c 'cp /tmp/iceberg_rest_mode\=memory /mnt/data/input/'

# save iceberg from s3
docker exec mc bash -c 'mc cp -r minio/warehouse /mnt/data/input/minio'
