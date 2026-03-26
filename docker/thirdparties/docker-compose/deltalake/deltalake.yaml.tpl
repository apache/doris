#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# NOTE: Delta Lake docker environment is integrated into the iceberg docker-compose.
# See docker/thirdparties/docker-compose/iceberg/iceberg.yaml.tpl
#
# Delta Lake shares the following services from the iceberg compose:
#   - spark-iceberg: Spark with delta-spark jars for creating Delta tables
#   - minio: S3-compatible storage for Delta table data
#   - postgres: PostgreSQL backing the Hive Metastore
#
# Additionally, the following service is added:
#   - hive-metastore: Standalone Hive Metastore for Delta Lake table registration
#
# To start the environment:
#   docker/thirdparties/run-thirdparties-docker.sh -c iceberg
#
# Delta Lake test data is created by SQL scripts in:
#   docker/thirdparties/docker-compose/iceberg/scripts/create_preinstalled_scripts/deltalake/
