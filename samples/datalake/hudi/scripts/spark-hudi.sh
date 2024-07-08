#!/usr/bin/env bash

export SPARK_HOME=/opt/spark-hudi
export HIVE_HOME=/opt/apache-hive-3.1.2-bin
export HADOOP_HOME=/opt/hadoop-3.3.1

if [ ! -d "$SPARK_HOME" ]; then
  cp -r /opt/spark-3.4.2-bin-hadoop3 $SPARK_HOME
fi

cp ${HIVE_HOME}/conf/hive-site.xml ${SPARK_HOME}/conf/
cp ${HIVE_HOME}/lib/postgresql-jdbc.jar ${SPARK_HOME}/jars/
cp ${HADOOP_HOME}/etc/hadoop/core-site.xml ${SPARK_HOME}/conf/

${SPARK_HOME}/bin/spark-sql \
  --master local[*] \
  --name "spark-hudi-sql" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalogImplementation=hive
