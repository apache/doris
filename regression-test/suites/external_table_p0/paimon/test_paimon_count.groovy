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

suite("test_paimon_count", "p0,external,doris,external_docker,external_docker_doris") {

    logger.info("start paimon test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    try {
        String catalog_name = "test_paimon_count"
        String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type" = "paimon",
            "paimon.catalog.type"="filesystem",
            "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
        );"""
        sql """use `${catalog_name}`.`db1`"""

        def test_cases = { String force ->
            sql """ set force_jni_scanner=${force} """
            qt_append_select """ select * from append_table order by product_id; """
            qt_append_count """ select count(*) from append_table; """
            qt_merge_on_read_select """ select * from merge_on_read_table order by product_id; """
            qt_merge_on_read_count """ select count(*) from merge_on_read_table; """
            qt_deletion_vector_select """ select * from deletion_vector_parquet order by id; """
            qt_deletion_vector_count """ select count(*) from deletion_vector_parquet; """
        }

        def test_table_count_push_down = { String force ->
            sql """ set force_jni_scanner=${force} """
            explain {
                sql("select count(*) from append_table;")
                contains "pushdown agg=COUNT (12)"
            }
            explain {
                sql("select count(*) from merge_on_read_table;")
                contains "pushdown agg=COUNT (8)"
            }
            explain {
                sql("select count(*) from deletion_vector_parquet;")
                contains "pushdown agg=COUNT (-1)"
            }
        }

        test_cases("false")
        test_cases("true")
        test_table_count_push_down("false")
        test_table_count_push_down("true")
    } finally {
        sql """set force_jni_scanner=false"""
    }
}