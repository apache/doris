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

suite("paimon_timestamp_types", "p2,external,paimon,external_remote,external_remote_paimon") {

    def ts_orc = """select * from ts_orc"""
    def ts_parquet = """select * from ts_parquet"""

    String enabled = context.config.otherConfigs.get("enableExternalPaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("enable_deprecated_case")) {
        // The timestamp type of paimon has no logical or converted type,
        // and is conflict with column type change from bigint to timestamp.
        // Deprecated currently.
        String catalog_name = "paimon_timestamp_catalog"
        String user_name = context.config.otherConfigs.get("extHiveHmsUser")
        String hiveHost = context.config.otherConfigs.get("extHiveHmsHost")
        String hivePort = context.config.otherConfigs.get("extHdfsPort")

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                "type" = "paimon",
                "paimon.catalog.type" = "filesystem",
                "warehouse" = "hdfs://${hiveHost}:${hivePort}/paimon/paimon1",
                "hadoop.username" = "${user_name}"
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use db1;"""
        logger.info("use db1")

        sql """set force_jni_scanner=true"""
        qt_c1 ts_orc
        qt_c2 ts_parquet

        sql """set force_jni_scanner=false"""
        qt_c3 ts_orc
        qt_c4 ts_parquet

    }
}

