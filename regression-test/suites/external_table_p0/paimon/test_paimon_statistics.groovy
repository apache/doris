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

suite("test_paimon_statistics", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
            String catalog_name = "ctl_test_paimon_statistics"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type" = "paimon",
                "paimon.catalog.type"="filesystem",
                "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
            );"""

            def table_id = get_table_id(catalog_name, "db1", "all_table")

            // analyze
            sql """use `${catalog_name}`.`db1`"""
            sql """analyze table all_table with sync"""

            // select
            def s1 = """select col_id,count,ndv,null_count,min,max,data_size_in_bytes from internal.__internal_schema.column_statistics where tbl_id = ${table_id} order by id;"""

            qt_s1 s1
        } finally {
        }
    }
}

