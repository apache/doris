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

"""Run after the SQL fixtures and the pinned Delta golden tables have been uploaded."""

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()
try:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    warehouse = "s3a://delta/warehouse"
    path = f"{warehouse}/delta_m1/log_compaction"
    engine = jvm.io.delta.kernel.defaults.engine.DefaultEngine.create(conf)
    snapshot = jvm.io.delta.kernel.TableManager.loadSnapshot(path).build(engine)
    version = snapshot.getVersion()
    compacted = jvm.org.apache.hadoop.fs.Path(
        f"{path}/_delta_log/{0:020d}.{version:020d}.compacted.json"
    )
    # Kernel 4.4.1 has no public log-compaction entry point. This fixture uses its pinned hook;
    # production internal API dependencies remain isolated in DeltaKernelCompat.
    if not compacted.getFileSystem(conf).exists(compacted):
        jvm.io.delta.kernel.internal.hook.LogCompactionHook(
            jvm.io.delta.kernel.internal.fs.Path(path),
            jvm.io.delta.kernel.internal.fs.Path(f"{path}/_delta_log"),
            0, version, 0,
        ).threadSafeInvoke(engine)
    print(f"Prepared log compaction through version {version}", flush=True)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS delta_golden LOCATION '{warehouse}/delta_golden'")
    tables = [
        "table_with_columnmapping_mode_name", "table_with_columnmapping_mode_id",
        "data_reader_timestamp_ntz", "data_reader_timestamp_ntz_name_mode", "data_reader_timestamp_ntz_id_mode",
        "type_widening", "type_widening_nested",
        "dv_partitioned_with_checkpoint", "dv_with_columnmapping", "log_replay_dv_key_cases",
        "multi_part_checkpoint", "only_checkpoint_files", "v2_checkpoint_json", "v2_checkpoint_parquet",
        "basic_with_vacuum_protocol_check_feature",
    ]
    for table in tables:
        spark.sql(f"CREATE TABLE IF NOT EXISTS delta_golden.{table} USING delta "
                  f"LOCATION '{warehouse}/delta_golden/{table}'")
finally:
    spark.stop()
