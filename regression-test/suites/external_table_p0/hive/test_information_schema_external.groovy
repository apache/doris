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

suite("test_information_schema_external", "p0,external,hive,external_docker,external_docker_hive") {
    // test  schemata 、columns、files、metadata_name_ids、partitions、tables、views
    //files  partitions no imp 

    def enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            def hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            def catalog_name = "test_information_schema_external_${hivePrefix}"
            def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""

            def db_name = "info_schema_ext_db"

            //schemata
            order_qt_schemata_1 """
                select * from ${catalog_name}.information_schema.schemata 
                    where  CATALOG_NAME = "${catalog_name}" and  SCHEMA_NAME = "default";
            """
            sql """ create database if not exists ${db_name}_1; """
            sql """ create database if not exists ${db_name}_2; """
            order_qt_schemata_2 """ 
                select * from internal.information_schema.schemata 
                    where  CATALOG_NAME = "internal" and  SCHEMA_NAME = "${db_name}_1";
            """
            order_qt_schemata_3 """ 
                select * from internal.information_schema.schemata 
                    where  CATALOG_NAME = "internal" and  SCHEMA_NAME = "${db_name}_2"; 
            """
            sql """ drop database if exists ${db_name}_1 """
            order_qt_schemata_4 """ 
                select * from internal.information_schema.schemata 
                    where  CATALOG_NAME = "internal" and  SCHEMA_NAME = "${db_name}_1";
            """
            order_qt_schemata_5 """ 
                select * from internal.information_schema.schemata 
                    where  CATALOG_NAME = "internal" and  SCHEMA_NAME = "${db_name}_2"; 
            """
            sql """ drop database if exists ${db_name}_2 """
            order_qt_schemata_6 """ 
                select * from internal.information_schema.schemata 
                    where  CATALOG_NAME = "internal" and  SCHEMA_NAME = "${db_name}_1";
            """
            order_qt_schemata_7 """ 
                select * from internal.information_schema.schemata 
                    where  CATALOG_NAME = "internal" and  SCHEMA_NAME = "${db_name}_2"; 
            """
            order_qt_schemata_8 """ 
                select * from internal.information_schema.schemata 
                    where  CATALOG_NAME = "internal" and  SCHEMA_NAME = "infomation_schema"; 
            """
            order_qt_schemata_9 """ 
                select * from internal.information_schema.schemata 
                    where  CATALOG_NAME = "internal" and  SCHEMA_NAME = "infomation_schema_ext"; 
            """
            
            sql """ drop database if exists ${db_name}_1 """            
            sql """ drop database if exists ${db_name}_2 """

            //columns
            sql """ create database if not exists ${db_name}; """
            sql """ drop table if exists  ${db_name}.abcd """
            sql """ 
            CREATE TABLE  ${db_name}.abcd (
                `id` int(11) not null ,
                `name` string
            )
            UNIQUE KEY(`id`) 
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES("replication_num" = "1");            
            """
            order_qt_columns_1 """
                select * from internal.information_schema.columns 
                    where TABLE_CATALOG = "internal" and  TABLE_SCHEMA = "${db_name}";
            """
            sql """ drop table if exists  ${db_name} """
            order_qt_columns_2 """
                select * from internal.information_schema.columns 
                    where TABLE_CATALOG = "internal" and  TABLE_SCHEMA = "${db_name}";
            """
            order_qt_columns_3 """
                select * from ${catalog_name}.information_schema.columns 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "customer";
            """
            order_qt_columns_4 """
                select * from ${catalog_name}.information_schema.columns 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "lineitem";
            """            
            order_qt_columns_5 """
                select * from ${catalog_name}.information_schema.columns 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "nation";
            """            
            order_qt_columns_6 """
                select * from ${catalog_name}.information_schema.columns 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "orders";
            """
            order_qt_columns_7 """
                select * from ${catalog_name}.information_schema.columns 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "partsupp";
            """
            sql """ drop table if exists  ${db_name}.abcd """
            sql """ drop database if  exists ${db_name}; """

            //metadata_name_ids
            order_qt_ids_1 """
                select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${catalog_name}.information_schema.metadata_name_ids 
                    where CATALOG_NAME = "${catalog_name}" and  DATABASE_NAME = "tpch1_parquet" and TABLE_NAME = "customer";
            """
            order_qt_ids_2 """
                select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${catalog_name}.information_schema.metadata_name_ids 
                    where CATALOG_NAME = "${catalog_name}" and  DATABASE_NAME = "tpch1_parquet" and TABLE_NAME = "lineitem";
            """            
            order_qt_ids_3 """
                select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${catalog_name}.information_schema.metadata_name_ids 
                    where CATALOG_NAME = "${catalog_name}" and  DATABASE_NAME = "tpch1_parquet" and TABLE_NAME = "nation";
            """            
            order_qt_ids_4 """
                select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${catalog_name}.information_schema.metadata_name_ids 
                    where CATALOG_NAME = "${catalog_name}" and  DATABASE_NAME = "tpch1_parquet" and TABLE_NAME = "orders";
            """            
            order_qt_ids_5 """
                select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${catalog_name}.information_schema.metadata_name_ids 
                    where CATALOG_NAME = "${catalog_name}" and  DATABASE_NAME = "tpch1_parquet" and TABLE_NAME = "part";
            """            
            order_qt_ids_6 """
                select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${catalog_name}.information_schema.metadata_name_ids 
                    where CATALOG_NAME = "${catalog_name}" and  DATABASE_NAME = "tpch1_parquet" and TABLE_NAME = "partsupp";
            """            
            order_qt_ids_7 """
                select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${catalog_name}.information_schema.metadata_name_ids 
                    where CATALOG_NAME = "${catalog_name}" and  DATABASE_NAME = "tpch1_parquet" and TABLE_NAME = "region";
            """            
            order_qt_ids_8 """
                select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${catalog_name}.information_schema.metadata_name_ids 
                    where CATALOG_NAME = "${catalog_name}" and  DATABASE_NAME = "tpch1_parquet" and TABLE_NAME = "supplier";
            """

            //tables
            order_qt_tables_1 """
                select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,DATA_LENGTH,MAX_DATA_LENGTH,TABLE_COMMENT from ${catalog_name}.information_schema.tables 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "supplier";
            """
            order_qt_tables_2 """
                select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,DATA_LENGTH,MAX_DATA_LENGTH,TABLE_COMMENT from ${catalog_name}.information_schema.tables 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "region";
            """
            order_qt_tables_3 """
                select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,DATA_LENGTH,MAX_DATA_LENGTH,TABLE_COMMENT from ${catalog_name}.information_schema.tables 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "customer";
            """
            order_qt_tables_4 """
                select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,DATA_LENGTH,MAX_DATA_LENGTH,TABLE_COMMENT from ${catalog_name}.information_schema.tables 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "lineitem";
            """            
            order_qt_tables_5 """
                select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,DATA_LENGTH,MAX_DATA_LENGTH,TABLE_COMMENT from ${catalog_name}.information_schema.tables 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "nation";
            """            
            order_qt_tables_6 """
                select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,DATA_LENGTH,MAX_DATA_LENGTH,TABLE_COMMENT from ${catalog_name}.information_schema.tables 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "orders";
            """
            order_qt_tables_7 """
                select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,DATA_LENGTH,MAX_DATA_LENGTH,TABLE_COMMENT from ${catalog_name}.information_schema.tables 
                    where TABLE_CATALOG = "${catalog_name}" and  TABLE_SCHEMA = "tpch1_parquet" and TABLE_NAME = "partsupp";
            """

            //views
            sql """ create database if not exists ${db_name}; """
            sql """ drop table if exists  ${db_name}.ab """
            sql """ 
            CREATE TABLE  ${db_name}.ab (
                `id` int(11) not null ,
                `name` string
            )
            UNIQUE KEY(`id`) 
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES("replication_num" = "1");            
            """
            sql """ drop VIEW IF  EXISTS ${db_name}.test_view """
            sql """
                CREATE VIEW IF NOT EXISTS ${db_name}.test_view (a)
                AS
                SELECT id as a FROM ${db_name}.ab
            """
            order_qt_views_1 """
                select * from internal.information_schema.views 
                    where  TABLE_SCHEMA = "${db_name}" and TABLE_NAME = "test_view";
            """
            sql """ drop VIEW IF  EXISTS ${db_name}.test_view """
            order_qt_views_2 """
                select * from internal.information_schema.views 
                    where  TABLE_SCHEMA = "${db_name}" and TABLE_NAME = "test_view";
            """
            sql """ drop VIEW IF  EXISTS ${db_name}.test_view """
            sql """ drop table if exists  ${db_name}.ab """
            sql """ drop database if  exists ${db_name}; """



            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
