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

suite("test_hive_migrate_paimon", "p2,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    enabled = context.config.otherConfigs.get("enableExternalEmrTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test enableExternalEmrTest = false")
        return
    }

    String props = context.config.otherConfigs.get("emrCatalogCommonProp")
    String catalog_name = "test_hive_migrate_paimon"
    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        "type" = "paimon",
        "paimon.catalog.type" = "hms",
        "warehouse" = "hdfs://master-1-1.c-a212282673679a24.cn-beijing.emr.aliyuncs.com:9000/user/hive/warehouse/",
        'hive.version' = '3.1.3',
        ${props}
    );"""
    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """ use regression_paimon;"""

    for (String jni : ["true","false"]) {
        sql  """ set force_jni_scanner= ${jni} """
        for (String table : ["hive_migrate_paimon_parquet", "hive_migrate_paimon_orc"]) {
            order_qt_all """ select * FROM ${table} ORDER BY id"""
            order_qt_1 """ select * FROM ${table} ORDER BY id limit 1"""
            order_qt_2 """ select * FROM ${table} ORDER BY dt limit 2"""
            order_qt_3 """ SELECT * FROM ${table} WHERE dt = '2026-04-01' LIMIT 10;"""
            order_qt_4 """ SELECT * FROM ${table} WHERE dt = '2026-04-02' LIMIT 1;"""
            order_qt_5 """ SELECT * FROM ${table} WHERE dt = '2026-04-03' LIMIT 1;"""
            order_qt_6 """ SELECT dt,id,user_name FROM ${table} WHERE dt = '2026-04-01' LIMIT 10;"""
            order_qt_7 """ SELECT id,dt,user_name FROM ${table} WHERE dt = '2026-04-02' LIMIT 1;"""
            order_qt_8 """ SELECT * FROM ${table} WHERE id = 1;"""
            order_qt_partition_only_1 """ SELECT dt FROM ${table} ORDER BY dt;"""
            order_qt_partition_only_2 """ SELECT DISTINCT dt FROM ${table} ORDER BY dt;"""
            order_qt_partition_only_3 """ SELECT dt FROM ${table} WHERE dt = '2026-04-02' ORDER BY dt;"""
            order_qt_partition_only_4 """ SELECT dt FROM ${table} WHERE dt IN ('2026-04-01', '2026-04-03') ORDER BY dt;"""
        }
    }

}

/*

hive :
CREATE TABLE hive_origin_parquet (
id BIGINT,
user_name STRING,
amount DECIMAL(10,2)
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

INSERT INTO hive_origin_parquet PARTITION (dt='2026-04-01')
SELECT 1, 'alice', CAST(10.50 AS DECIMAL(10,2));
INSERT INTO hive_origin_parquet PARTITION (dt='2026-04-01')
SELECT 2, 'bob', CAST(20.00 AS DECIMAL(10,2));
INSERT INTO hive_origin_parquet PARTITION (dt='2026-04-02')
SELECT 3, 'carol', CAST(30.25 AS DECIMAL(10,2));


spark-sql  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions :

CALL paimon.sys.migrate_table(
source_type => 'hive',
table => 'regression_paimon.hive_origin_parquet',
target_table => 'regression_paimon.hive_migrate_paimon_parquet',
delete_origin => false,
options => 'file.format=parquet',
parallelism => 4
);

insert into hive_migrate_paimon_parquet values (4, 'dva',   CAST(40.00 AS DECIMAL(10,2)), '2026-04-03');

*/
