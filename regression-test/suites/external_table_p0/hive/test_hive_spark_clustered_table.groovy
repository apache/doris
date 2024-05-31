// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_hive_spark_clustered_table", "p0,external,hive,external_docker,external_docker_hive") {
    def q01 = {
        qt_q01 """ select * from parquet_test2 t1, parquet_test2 t2 WHERE t1.user_id = t2.user_id ORDER BY 1,2 ;"""

        qt_q02 """explain select * from parquet_test2 t1, parquet_test2 t2 WHERE t1.user_id = t2.user_id ;"""

        qt_q03 """select * from parquet_test2 t1, `internal`.`regression_test`.doris_dist_test t2 WHERE t1.user_id = t2.user_id ORDER BY 1,2 ;"""

        explain {
             sql("""select * from parquet_test2 t1, `internal`.`regression_test`.doris_dist_test t2 WHERE t1.user_id = t2.user_id;""")
             contains "join op: INNER JOIN(BUCKET_SHUFFLE)"
             contains "BUCKET_SHFFULE_HASH_PARTITIONED(SPARK_MURMUR32)"
        }
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String hms_port = context.config.otherConfigs.get("hms_port")
            String catalog_name = "hive_test_parquet"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""

            sql """use `regression_test`"""
            sql """drop table if exists doris_dist_test;"""
            sql """create table doris_dist_test properties("replication_num"="1")
                     as select * from `${catalog_name}`.`default`.parquet_test2; """

            sql """use `${catalog_name}`.`default`"""

            sql """set enable_fallback_to_original_planner=false;"""
            sql """SET enable_spark_bucket_shuffle=true"""

            q01()

            sql """set enable_nereids_planner=false;"""

            q01()

            sql """use `internal`.`regression_test`"""
            sql """drop table if exists doris_dist_test; """
            sql """drop catalog if exists ${catalog_name}; """
        } finally {
        }
    }
}
