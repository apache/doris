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

import org.junit.Assert;

suite("test_two_iceberg_kerberos", "p0,external,kerberos,external_docker,external_docker_kerberos") {
    String enabled = context.config.otherConfigs.get("enableKerberosTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // test iceberg hms catalog with kerberos
        sql """
            CREATE CATALOG IF NOT EXISTS test_krb_iceberg_ctl
            PROPERTIES (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                "hive.metastore.uris" = "thrift://172.31.71.25:9083",
                "fs.defaultFS" = "hdfs://hadoop-master:9000",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "/keytabs/hive-presto-master.keytab",
                "hive.metastore.sasl.enabled" = "true",
                "hive.metastore.kerberos.principal" = "hive/_HOST@LABS.TERADATA.COM",
                "hive.metastore.warehouse.dir"="hdfs://hadoop-master:9000/user/hive/warehouse",
                "doris.krb5.debug" = "true"
            );
        """

        sql """
            CREATE CATALOG IF NOT EXISTS other_test_krb_iceberg_ctl
            PROPERTIES (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                "hive.metastore.uris" = "thrift://172.31.71.26:9083",
                "fs.defaultFS" = "hdfs://hadoop-master-2:9000",
                "hive.metastore.warehouse.dir"="hdfs://hadoop-master-2:9000/user/hive/warehouse",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@OTHERREALM.COM",
                "hadoop.kerberos.keytab" = "/keytabs/other-hive-presto-master.keytab",
                "hive.metastore.sasl.enabled" = "true",
                "hive.metastore.kerberos.principal" = "hive/_HOST@OTHERREALM.COM",
                "hadoop.security.auth_to_local" ="RULE:[2:\$1@\$0](.*@OTHERREALM.COM)s/@.*//
                                                  RULE:[2:\$1@\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                                  DEFAULT",
                "doris.krb5.debug" = "true"
            );
        """

        sql """ SWITCH test_krb_iceberg_ctl; """
        sql """ CREATE DATABASE IF NOT EXISTS `test_krb_iceberg_db`; """
        sql """ USE `test_krb_iceberg_db`; """
        sql """ CREATE TABLE IF NOT EXISTS test_krb_iceberg_tbl (id int, str string, dd date) engine = iceberg; """
        sql """ INSERT INTO test_krb_iceberg_tbl values(1, 'krb1', '2023-05-14') """
        sql """ INSERT INTO test_krb_iceberg_tbl values(2, 'krb2', '2023-05-16') """
        sql """ INSERT INTO test_krb_iceberg_tbl values(3, 'krb3', '2023-05-17') """
        order_qt_iceberg_q01 """ SELECT * FROM test_krb_iceberg_tbl """

        sql """ SWITCH other_test_krb_iceberg_ctl; """
        sql """ CREATE DATABASE IF NOT EXISTS `other_test_krb_iceberg_db`; """
        sql """ USE `other_test_krb_iceberg_db`; """
        sql """ CREATE TABLE IF NOT EXISTS other_test_krb_iceberg_tbl (id int, str string, dd date) engine = iceberg; """
        sql """ INSERT INTO other_test_krb_iceberg_tbl values(1, 'krb1', '2023-05-14') """
        sql """ INSERT INTO other_test_krb_iceberg_tbl values(2, 'krb2', '2023-05-16') """
        sql """ INSERT INTO other_test_krb_iceberg_tbl values(3, 'krb3', '2023-05-17') """

        //  test iceberg hadoop catalog with kerberos
        sql """
            CREATE CATALOG IF NOT EXISTS test_krb_iceberg_ctl_hadoop
            PROPERTIES ( 
                'type'='iceberg',
                'iceberg.catalog.type'='hadoop',
                "warehouse" = "hdfs://hadoop-master:9000/user/hive/",
                "fs.defaultFS" = "hdfs://hadoop-master:9000",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "/keytabs/hive-presto-master.keytab",
                "doris.krb5.debug" = "true"
            );
            """

        sql """ SWITCH test_krb_iceberg_ctl_hadoop; """
        sql """ CREATE DATABASE IF NOT EXISTS hadoop_test_krb_iceberg_db; """
        sql """ USE hadoop_test_krb_iceberg_db; """
        sql """ CREATE TABLE IF NOT EXISTS hadoop_test_krb_iceberg_tbl (id int, str string, dd date) engine = iceberg; """
        sql """ INSERT INTO hadoop_test_krb_iceberg_tbl values(1, 'krb1', '2023-05-14') """
        sql """ INSERT INTO hadoop_test_krb_iceberg_tbl values(2, 'krb2', '2023-05-16') """
        sql """ INSERT INTO hadoop_test_krb_iceberg_tbl values(3, 'krb3', '2023-05-17') """

        order_qt_iceberg_q02 """ SELECT id,dd FROM test_krb_iceberg_ctl.test_krb_iceberg_db.test_krb_iceberg_tbl where dd >= '2023-05-16' """
        order_qt_iceberg_q03 """ SELECT id,dd FROM other_test_krb_iceberg_ctl.other_test_krb_iceberg_db.other_test_krb_iceberg_tbl where dd <= '2023-05-16' """

        // cross catalog query test
        order_qt_iceberg_q04 """ SELECT id,dd FROM hadoop_test_krb_iceberg_tbl where dd <= '2023-05-16' """
        order_qt_iceberg_q05 """ SELECT * FROM test_krb_iceberg_ctl.test_krb_iceberg_db.test_krb_iceberg_tbl """
        order_qt_iceberg_q06 """ SELECT * FROM test_krb_iceberg_ctl_hadoop.hadoop_test_krb_iceberg_db.hadoop_test_krb_iceberg_tbl """
        order_qt_iceberg_q07 """ SELECT * FROM other_test_krb_iceberg_ctl.other_test_krb_iceberg_db.other_test_krb_iceberg_tbl """

        sql """ DROP TABLE IF EXISTS test_krb_iceberg_ctl.`test_krb_iceberg_db`.`test_krb_iceberg_tbl`; """
        sql """ DROP TABLE IF EXISTS other_test_krb_iceberg_ctl.`other_test_krb_iceberg_db`.`other_test_krb_iceberg_tbl`; """
        sql """ DROP TABLE IF EXISTS test_krb_iceberg_ctl_hadoop.`hadoop_test_krb_iceberg_db`.`hadoop_test_krb_iceberg_tbl`; """

        sql """ DROP DATABASE IF EXISTS test_krb_iceberg_ctl.`test_krb_iceberg_db`; """
        sql """ DROP DATABASE IF EXISTS other_test_krb_iceberg_ctl.`other_test_krb_iceberg_db`; """
        sql """ DROP DATABASE IF EXISTS test_krb_iceberg_ctl_hadoop.`hadoop_test_krb_iceberg_db`; """

        sql """ DROP CATALOG test_krb_iceberg_ctl """
        sql """ DROP CATALOG other_test_krb_iceberg_ctl """
        sql """ DROP CATALOG test_krb_iceberg_ctl_hadoop """
    }
}
