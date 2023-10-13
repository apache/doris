echo "copy db"
ls -l /tmp/iceberg_rest_mode=memory
cp /mnt/data/input/iceberg_rest_mode=memory /tmp/iceberg_rest_mode=memory
ls -l /tmp/iceberg_rest_mode=memory

java -jar iceberg-rest-image-all.jar
