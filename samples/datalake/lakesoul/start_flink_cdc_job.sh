#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

echo "Start flink-cdc job..."
sudo docker exec -t doris-lakesoul-jobmanager flink run -d -c org.apache.flink.lakesoul.entry.MysqlCdc /opt/flink/jars/lakesoul-flink-1.17-2.6.1.jar --source_db.host mysql --source_db.port 3306 --source_db.db_name tpch --source_db.user root --source_db.password root --source.parallelism 2 --sink.parallelism 4 --use.cdc true --warehouse_path s3://lakesoul-test-bucket/data/ --flink.checkpoint s3://lakesoul-test-bucket/chk --flink.savepoint s3://lakesoul-test-bucket/svp --job.checkpoint_interval 5000 --server_time_zone UTC
