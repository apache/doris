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

suite("test_deltalake_basic", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    sql "set time_zone='UTC'"
    for (String source : ["filesystem", "hms"]) {
        String catalog = "test_deltalake_basic_${source}"
        sql "drop catalog if exists ${catalog}"
        String metastore = source == "filesystem" ? "'warehouse'='${warehouse}'"
                : "'hive.metastore.uris'='thrift://${host}:${hmsPort}'"
        sql """create catalog ${catalog} properties (
            'type'='deltalake', 'deltalake.catalog.type'='${source}', ${metastore},
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""
        sql "refresh catalog ${catalog}"
        "order_qt_${source}_tables"("show tables from ${catalog}.delta_m1 like 'basic'")
        "qt_${source}_schema"("desc ${catalog}.delta_m1.basic")
        for (boolean v2 : [true, false]) {
            sql "set enable_file_scanner_v2=${v2}"
            String label = "${source}_${v2}"
            "order_qt_${label}_basic"("select * from ${catalog}.delta_m1.basic order by id")
            "qt_${label}_count"("select count(*) from ${catalog}.delta_m1.basic")
            "order_qt_${label}_null_partition"(
                    "select * from ${catalog}.delta_m1.basic where part is null order by id")
            "order_qt_${label}_one_partition"(
                    "select * from ${catalog}.delta_m1.basic where part='a' order by id")
            "order_qt_${label}_no_partition"(
                    "select * from ${catalog}.delta_m1.basic where part='missing' order by id")
            "order_qt_${label}_empty"("select * from ${catalog}.delta_m1.empty order by id")
            "qt_${label}_empty_count"("select count(*) from ${catalog}.delta_m1.empty")
            "order_qt_${label}_types"("""select id,b,i8,i16,i32,i64,f,d,dec,str,hex(bin),dt,ts,arr,m,nested
                    from ${catalog}.delta_m1.types order by id""")
            "order_qt_${label}_partition_types"(
                    "select * from ${catalog}.delta_m1.partition_types order by id")
            explain {
                sql("select * from ${catalog}.delta_m1.basic where part='a'")
                contains("partition=1/3")
                contains("inputSplitNum=1")
            }
        }
        sql "set file_split_size=128"
        "order_qt_${source}_byte_ranges"("""select id,b,i8,i16,i32,i64,f,d,dec,str,hex(bin),dt,ts,arr,m,nested
                from ${catalog}.delta_m1.types order by id""")
        sql "set file_split_size=0"
        for (String zone : ["UTC", "Asia/Shanghai", "America/Los_Angeles"]) {
            sql "set time_zone='${zone}'"
            String zoneLabel = zone.replace('/', '_')
            "order_qt_${source}_${zoneLabel}_timestamps"(
                    "select id,cast(ts as datetimev2(6)) from ${catalog}.delta_m1.types order by id")
            "order_qt_${source}_${zoneLabel}_partition_timestamps"(
                    "select id,cast(p_ts as datetimev2(6)) from ${catalog}.delta_m1.partition_types order by id")
        }
        sql "set time_zone='UTC'"
        test {
            sql "select * from ${catalog}.delta_m1.missing_table"
            exception "does not exist"
        }
    }
}
