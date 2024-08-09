import groovyjarjarantlr4.v4.codegen.model.ExceptionClause

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

suite("test_two_hive_kerberos", "p0,external,kerberos,external_docker,external_docker_kerberos") {
    String enabled = context.config.otherConfigs.get("enableKerberosTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_catalog_name = "test_two_hive_kerberos"
        sql """drop catalog if exists ${hms_catalog_name};"""
        sql """
            CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
            PROPERTIES ( 
                "type" = "hms",
                "hive.metastore.uris" = "thrift://172.31.71.25:9083",
                "fs.defaultFS" = "hdfs://172.31.71.25:8020",
                "hadoop.kerberos.min.seconds.before.relogin" = "5",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "/keytabs/hive-presto-master.keytab",
                "hive.metastore.sasl.enabled " = "true",
                "hive.metastore.kerberos.principal" = "hive/_HOST@LABS.TERADATA.COM"
            );
        """

        sql """drop catalog if exists other_${hms_catalog_name};"""
        sql """
            CREATE CATALOG IF NOT EXISTS other_${hms_catalog_name}
            PROPERTIES (
                "type" = "hms",
                "hive.metastore.uris" = "thrift://172.31.71.26:9083",
                "fs.defaultFS" = "hdfs://172.31.71.26:8020",
                "hadoop.kerberos.min.seconds.before.relogin" = "5",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@OTHERREALM.COM",
                "hadoop.kerberos.keytab" = "/keytabs/other-hive-presto-master.keytab",
                "hive.metastore.sasl.enabled " = "true",
                "hive.metastore.kerberos.principal" = "hive/_HOST@OTHERREALM.COM",
                "hadoop.security.auth_to_local" ="RULE:[2:\$1@\$0](.*@OTHERREALM.COM)s/@.*//
                                                  RULE:[2:\$1@\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                                  DEFAULT"
            );
        """

        // 1. catalogA
        sql """switch ${hms_catalog_name};"""
        logger.info("switched to catalog " + hms_catalog_name)
        sql """ show databases """
        sql """ use test_krb_hive_db """
        order_qt_q01 """ select * from test_krb_hive_db.test_krb_hive_tbl """

        // 2. catalogB
        sql """switch other_${hms_catalog_name};"""
        logger.info("switched to other catalog " + hms_catalog_name)
        sql """ show databases """
        sql """ use test_krb_hive_db """
        order_qt_q02 """ select * from test_krb_hive_db.test_krb_hive_tbl """

        // 3. write back test case
        sql """ switch ${hms_catalog_name}; """
        sql """ CREATE DATABASE IF NOT EXISTS `test_krb_hms_db`; """
        sql """ USE `test_krb_hms_db`; """
        sql """ CREATE TABLE IF NOT EXISTS test_krb_hive_tbl (id int, str string, dd date) engine = hive; """
        sql """ INSERT INTO test_krb_hms_db.test_krb_hive_tbl values(1, 'krb1', '2023-05-14') """
        sql """ INSERT INTO test_krb_hms_db.test_krb_hive_tbl values(2, 'krb2', '2023-05-24') """

        sql """ switch other_${hms_catalog_name}; """
        sql """ CREATE DATABASE IF NOT EXISTS `test_krb_hms_db`; """
        sql """ USE `test_krb_hms_db`; """
        sql """ CREATE TABLE IF NOT EXISTS test_krb_hive_tbl (id int, str string, dd date) engine = hive; """
        sql """ INSERT INTO test_krb_hms_db.test_krb_hive_tbl values(1, 'krb1', '2023-05-24') """
        sql """ INSERT INTO test_krb_hms_db.test_krb_hive_tbl values(2, 'krb2', '2023-05-24') """

        sql """ INSERT INTO ${hms_catalog_name}.test_krb_hms_db.test_krb_hive_tbl values(3, 'krb3', '2023-06-14') """
        sql """ INSERT INTO other_${hms_catalog_name}.test_krb_hms_db.test_krb_hive_tbl values(6, 'krb3', '2023-09-14') """
        order_qt_q03 """ select * from ${hms_catalog_name}.test_krb_hms_db.test_krb_hive_tbl """
        order_qt_q04 """ select * from other_${hms_catalog_name}.test_krb_hms_db.test_krb_hive_tbl """

        // 4. multi thread test
        Thread thread1 = new Thread(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    sql """ select * from ${hms_catalog_name}.test_krb_hive_db.test_krb_hive_tbl """
                    sql """ INSERT INTO ${hms_catalog_name}.test_krb_hms_db.test_krb_hive_tbl values(3, 'krb3', '2023-06-14') """
                }
            } catch (Exception e) {
                log.info(e.getMessage())
                Assert.fail();
            }
        })

        Thread thread2 = new Thread(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    sql """ select * from other_${hms_catalog_name}.test_krb_hive_db.test_krb_hive_tbl """
                    sql """ INSERT INTO other_${hms_catalog_name}.test_krb_hms_db.test_krb_hive_tbl values(6, 'krb3', '2023-09-14') """
                }
            } catch (Exception e) {
                log.info(e.getMessage())
                Assert.fail();
            }
        })
        sleep(5000L)
        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()
        sql """drop catalog ${hms_catalog_name};"""
        sql """drop catalog other_${hms_catalog_name};"""
    }
}
