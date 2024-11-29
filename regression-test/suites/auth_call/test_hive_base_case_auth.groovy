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

suite("test_hive_base_case_auth", "p0,auth_call") {

    String user = 'test_hive_base_case_auth_user'
    String pwd = 'C123_567p'
    String catalogName = 'test_hive_base_case_auth_catalog'
    String dbName = 'test_hive_base_case_auth_db'
    String tableName = 'test_hive_base_case_auth_tb'
    String tableNameNew = 'test_hive_base_case_auth_tb_new'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)

        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        try_sql("DROP USER ${user}")
        try_sql """drop catalog if exists ${catalogName}"""
        try_sql """drop database if exists ${dbName}"""
        sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
        sql """grant select_priv on regression_test to ${user}"""

        // create catalog
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            test {
                sql """create catalog if not exists ${catalogName} properties (
                    'type'='hms'
                );"""
                exception "denied"
            }
            def ctl_res = sql """show catalogs;"""
            assertTrue(ctl_res.size() == 1)
        }
        sql """create catalog if not exists ${catalogName} properties (
            'type'='hms'
        );"""
        sql """grant Create_priv on ${catalogName}.*.* to ${user}"""
        try_sql """drop catalog if exists ${catalogName}"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql """create catalog if not exists ${catalogName} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""
            sql """show create catalog ${catalogName}"""
            def ctl_res = sql """show catalogs;"""
            assertTrue(ctl_res.size() == 2)
        }
        sql """revoke Create_priv on ${catalogName}.*.* from ${user}"""

        // create database
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            test {
                sql """create database ${catalogName}.${dbName};"""
                exception "denied"
            }
        }
        sql """create database ${catalogName}.${dbName};"""
        sql """grant Create_priv on ${catalogName}.${dbName}.* to ${user}"""
        sql """drop table if exists ${catalogName}.${dbName}.${tableName};"""
        sql """drop database ${catalogName}.${dbName};"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql """create database ${catalogName}.${dbName};"""
        }
        sql """revoke Create_priv on ${catalogName}.${dbName}.* from ${user}"""

        // create table
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            test {
                sql """create table ${catalogName}.${dbName}.${tableName} (
                    id BIGINT,
                    username VARCHAR(20)
                ) ENGINE=hive
                PROPERTIES (
                  'file_format'='parquet'
                );"""
                exception "denied"
            }
        }
        sql """create table ${catalogName}.${dbName}.${tableName} (
            id BIGINT,
            username VARCHAR(20)
        ) ENGINE=hive
        PROPERTIES (
          'file_format'='parquet'
        );"""
        sql """grant Create_priv on ${catalogName}.${dbName}.${tableName} to ${user}"""
        sql """drop table ${catalogName}.${dbName}.${tableName}"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql """create table ${catalogName}.${dbName}.${tableName} (
                id BIGINT,
                username VARCHAR(20)
            ) ENGINE=hive
            PROPERTIES (
              'file_format'='parquet'
            );"""
            sql """switch ${catalogName}"""
            sql """use ${dbName}"""
            sql """show create table ${tableName}"""
            def db_res = sql """show tables;"""
            assertTrue(db_res.size() == 1)
        }
        sql """revoke Create_priv on ${catalogName}.${dbName}.${tableName} from ${user}"""

        // load
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            test {
                sql """
                insert into ${catalogName}.${dbName}.${tableName} values 
                (1, "111"),
                (2, "222");
                """
                exception "denied"
            }
        }
        sql """grant LOAD_PRIV on ${catalogName}.${dbName}.${tableName} to ${user}"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql """
            insert into ${catalogName}.${dbName}.${tableName} values 
            (1, "111"),
            (2, "222");
            """
        }
        sql """revoke LOAD_PRIV on ${catalogName}.${dbName}.${tableName} from ${user}"""

        // alter
//        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
//            test {
//                sql """ALTER table ${catalogName}.${dbName}.${tableName} RENAME ${tableNameNew};"""
//                exception "denied"
//            }
//        }
//        sql """grant ALTER_PRIV on ${catalogName}.${dbName}.${tableName} to ${user}"""
//        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
//            sql """ALTER table ${catalogName}.${dbName}.${tableName} RENAME ${tableNameNew};"""
//        }
//        sql """revoke ALTER_PRIV on ${catalogName}.${dbName}.${tableName} from ${user}"""
//        sql """ALTER table ${catalogName}.${dbName}.${tableNameNew} RENAME ${tableName};"""

        // drop
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            test {
                sql """drop catalog ${catalogName}"""
                exception "denied"
            }
            test {
                sql """drop database ${catalogName}.${dbName}"""
                exception "denied"
            }
            test {
                sql """drop table ${catalogName}.${dbName}.${tableName}"""
                exception "denied"
            }
        }
        sql """grant DROP_PRIV on ${catalogName}.*.* to ${user}"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql """drop table ${catalogName}.${dbName}.${tableName}"""
            sql """drop database ${catalogName}.${dbName}"""
            sql """drop catalog ${catalogName}"""
        }

        sql """drop catalog if exists ${catalogName}"""
        try_sql("DROP USER ${user}")
    }



}
