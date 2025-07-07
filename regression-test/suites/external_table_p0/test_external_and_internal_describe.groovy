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

suite("test_external_and_internal_describe", "p0,external,hive,external_docker,external_docker_hive") {
    String catalog_name = "test_test_external_describe"

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        for (String hivePrefix : ["hive3"]) {
            setHivePrefix(hivePrefix)
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")

            // 1. test default catalog
            sql """drop catalog if exists ${catalog_name};"""
            sql """
            create catalog ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );
            """
            sql """switch ${catalog_name}"""
            hive_docker """drop database if exists test_external_describe CASCADE"""
            hive_docker """create database test_external_describe"""
            hive_docker """
                CREATE TABLE IF NOT EXISTS test_external_describe.user_behavior_log (
                    user_id STRING COMMENT 'Unique identifier for the user',
                    item_id STRING COMMENT 'Unique identifier for the product',
                    category_id STRING COMMENT 'Category ID of the product',
                    behavior_type STRING COMMENT '行为类型，包含pv(浏览)、buy(购买)、cart(加购)、fav(收藏)',
                    user_age TINYINT COMMENT 'Age group of the user (1=18-25, 2=26-30, 3=31-35, 4=36-40, 5=40+)',
                    gender STRING COMMENT 'User gender (0=female, 1=male, null=unknown)',
                    province STRING COMMENT 'Province where the user is located',
                    city STRING COMMENT 'City where the user is located',
                    behavior_time TIMESTAMP COMMENT 'Timestamp when the behavior occurred',
                    behavior_date DATE COMMENT 'Date when the behavior occurred'
                )
                COMMENT 'Log table for user behaviors'
                PARTITIONED BY (dt STRING COMMENT 'Partition field in yyyy-MM-dd format')
                STORED AS PARQUET
            """
            // no comment
            sql """set show_column_comment_in_describe = false"""
            qt_desc01 """desc test_external_describe.user_behavior_log"""
            // set show comment
            sql """set show_column_comment_in_describe = true"""
            qt_desc02 """desc test_external_describe.user_behavior_log"""

            sql """unset variable show_column_comment_in_describe;"""

            // test show proc
            def show_proc_string = """/catalogs/"""
            List<List<Object>> res = sql """show proc '${show_proc_string}'"""
            for (int i = 0; i < res.size(); i++) {
                if (res[i][1].equals("test_test_external_describe")) {
                    show_proc_string = """${show_proc_string}${res[i][0]}/"""
                }
            }
            // show proc "/catalogs/1747727318719/"
            res = sql """show proc '${show_proc_string}'"""
            for (int i = 0; i < res.size(); i++) {
                if (res[i][1].equals("test_external_describe")) {
                    show_proc_string = """${show_proc_string}${res[i][0]}/"""
                }
            }
            // show proc "/catalogs/1747727318719/2272230635936012419/"
            res = sql """show proc '${show_proc_string}'"""
            for (int i = 0; i < res.size(); i++) {
                if (res[i][1].equals("user_behavior_log")) {
                    show_proc_string = """${show_proc_string}${res[i][0]}/index_schema/"""
                }
            }
            // show proc "/catalogs/1747727318719/2272230635936012419/4443123596601666371/index_schema"
            res = sql """show proc '${show_proc_string}'"""
            for (int i = 0; i < res.size(); i++) {
                if (res[i][1].equals("user_behavior_log")) {
                    show_proc_string = """${show_proc_string}${res[i][0]}"""
                }
            }
            sql """set show_column_comment_in_describe = false"""
            qt_proc_sql01 """show proc '${show_proc_string}'"""
            sql """set show_column_comment_in_describe = true""" 
            qt_proc_sql02 """show proc '${show_proc_string}'"""

            sql """unset variable show_column_comment_in_describe;"""
            // sql """drop table test_external_describe.user_behavior_log"""
        }

        // desc internal table
        sql "switch internal"
        sql "drop database if exists test_external_and_internal_describe_db"
        sql "create database test_external_and_internal_describe_db";
        sql "use test_external_and_internal_describe_db";
        sql """
            CREATE TABLE test_external_and_internal_describe_tbl (
                k1 int COMMENT "first column",
                k2 string COMMENT "",
                k3 string COMMENT "中文column"
            ) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES("replication_num" = "1");
        """

        // no comment
        sql """set show_column_comment_in_describe = false"""
        qt_desc01 """desc test_external_and_internal_describe_tbl"""
        // set show comment
        sql """set show_column_comment_in_describe = true"""
        qt_desc02 """desc test_external_and_internal_describe_tbl"""

        // test show proc for internal
        def show_proc_string = """/catalogs/0/"""
        def show_proc_db_string = """/dbs/"""
        List<List<Object>> res = sql """show proc '${show_proc_string}'"""
        for (int i = 0; i < res.size(); i++) {
            if (res[i][1].equals("test_external_and_internal_describe_db")) {
                show_proc_string = """${show_proc_string}${res[i][0]}/"""
                show_proc_db_string = """${show_proc_db_string}${res[i][0]}/"""
            }
        }
        // show proc "/catalogs/0/1747727318719/"
        res = sql """show proc '${show_proc_string}'"""
        for (int i = 0; i < res.size(); i++) {
            if (res[i][1].equals("test_external_and_internal_describe_tbl")) {
                show_proc_string = """${show_proc_string}${res[i][0]}/index_schema/"""
                show_proc_db_string = """${show_proc_db_string}${res[i][0]}/index_schema/"""
            }
        }
        // show proc "/catalogs/0/1747727318719/2272230635936012419/4443123596601666371/index_schema"
        res = sql """show proc '${show_proc_string}'"""
        for (int i = 0; i < res.size(); i++) {
            if (res[i][1].equals("test_external_and_internal_describe_tbl")) {
                show_proc_string = """${show_proc_string}${res[i][0]}"""
                show_proc_db_string = """${show_proc_db_string}${res[i][0]}"""
            }
        }
        sql """set show_column_comment_in_describe = false"""
        qt_proc_sql01 """show proc '${show_proc_string}'"""
        qt_proc_db_sql01 """show proc '${show_proc_db_string}'"""
        sql """set show_column_comment_in_describe = true""" 
        qt_proc_sql02 """show proc '${show_proc_string}'"""
        qt_proc_db_sql02 """show proc '${show_proc_db_string}'"""


        sql """unset variable show_column_comment_in_describe;"""
    }
}

