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
import java.util.concurrent.*

suite("test_two_hive_kerberos", "p0,external,kerberos,external_docker,external_docker_kerberos") {
    def command = "sudo docker ps"
    def process = command.execute() 
    process.waitFor()               
    
    def output = process.in.text

    def keytab_root_dir = "/keytabs"
    
    println "Docker containers:"
    println output
    String enabled = context.config.otherConfigs.get("enableKerberosTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_catalog_name = "test_two_hive_kerberos"
        sql """drop catalog if exists ${hms_catalog_name};"""
        sql """
            CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
            PROPERTIES ( 
                "type" = "hms",
                "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
                "fs.defaultFS" = "hdfs://${externalEnvIp}:8520",
                "hadoop.kerberos.min.seconds.before.relogin" = "5",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab",
                "hive.metastore.sasl.enabled " = "true",
                "hadoop.security.auth_to_local" = "RULE:[2:\\\$1@\\\$0](.*@LABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERREALM.COM)s/@.*//
                                   DEFAULT",
                "hive.metastore.kerberos.principal" = "hive/hadoop-master@LABS.TERADATA.COM"
            );
        """

        sql """drop catalog if exists other_${hms_catalog_name};"""
        sql """
            CREATE CATALOG IF NOT EXISTS other_${hms_catalog_name}
            PROPERTIES (
                "type" = "hms",
                "hive.metastore.uris" = "thrift://${externalEnvIp}:9683",
                "fs.defaultFS" = "hdfs://${externalEnvIp}:8620",
                "hadoop.kerberos.min.seconds.before.relogin" = "5",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@OTHERREALM.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/other-hive-presto-master.keytab",
                "hive.metastore.sasl.enabled " = "true",
                "hive.metastore.kerberos.principal" = "hive/hadoop-master-2@OTHERREALM.COM",
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
        sql """ CREATE DATABASE IF NOT EXISTS `write_back_krb_hms_db`; """
        sql """ USE `write_back_krb_hms_db`; """
        sql """ DROP TABLE IF EXISTS test_krb_hive_tbl"""
        sql """ CREATE TABLE test_krb_hive_tbl (id int, str string, dd date) engine = hive; """
        sql """ INSERT INTO write_back_krb_hms_db.test_krb_hive_tbl values(1, 'krb1', '2023-05-14') """
        sql """ INSERT INTO write_back_krb_hms_db.test_krb_hive_tbl values(2, 'krb2', '2023-05-24') """

        sql """ switch other_${hms_catalog_name}; """
        sql """ CREATE DATABASE IF NOT EXISTS `write_back_krb_hms_db`; """
        sql """ USE `write_back_krb_hms_db`; """
        sql """ DROP TABLE IF EXISTS test_krb_hive_tbl"""
        sql """ CREATE TABLE test_krb_hive_tbl (id int, str string, dd date) engine = hive; """
        sql """ INSERT INTO write_back_krb_hms_db.test_krb_hive_tbl values(1, 'krb1', '2023-05-24') """
        sql """ INSERT INTO write_back_krb_hms_db.test_krb_hive_tbl values(2, 'krb2', '2023-05-24') """

        sql """ INSERT INTO ${hms_catalog_name}.write_back_krb_hms_db.test_krb_hive_tbl values(3, 'krb3', '2023-06-14') """
        sql """ INSERT INTO other_${hms_catalog_name}.write_back_krb_hms_db.test_krb_hive_tbl values(6, 'krb3', '2023-09-14') """
        order_qt_q03 """ select * from ${hms_catalog_name}.write_back_krb_hms_db.test_krb_hive_tbl """
        order_qt_q04 """ select * from other_${hms_catalog_name}.write_back_krb_hms_db.test_krb_hive_tbl """

        // 4. multi thread test
        def executor = Executors.newFixedThreadPool(2)

        def task1 = executor.submit({
            for (int i = 0; i < 100; i++) {
                sql """ select * from ${hms_catalog_name}.test_krb_hive_db.test_krb_hive_tbl """
                sql """ INSERT INTO ${hms_catalog_name}.write_back_krb_hms_db.test_krb_hive_tbl values(3, 'krb3', '2023-06-14') """
            }
        })

        def task2 = executor.submit({
            for (int i = 0; i < 100; i++) {
                sql """ select * from other_${hms_catalog_name}.test_krb_hive_db.test_krb_hive_tbl """
                sql """ INSERT INTO other_${hms_catalog_name}.write_back_krb_hms_db.test_krb_hive_tbl values(6, 'krb3', '2023-09-14') """
            }
        })
        
        try {
            task1.get()
            task2.get()
        } catch (ExecutionException e) {
            throw new AssertionError("Task failed", e.getCause())
        } finally {
            executor.shutdown()
        }
        // // test information_schema.backend_kerberos_ticket_cache
        // sql """switch internal"""
        // List<List<Object>> backends = sql "show backends"
        // int beNum = backends.size();
        // test {
        //     sql """select * from information_schema.backend_kerberos_ticket_cache where PRINCIPAL="hive/presto-master.docker.cluster@LABS.TERADATA.COM" and KEYTAB = "${keytab_root_dir}/hive-presto-master.keytab";"""
        //     rowNum beNum
        // } 

        // test {
        //     sql """select * from information_schema.backend_kerberos_ticket_cache where PRINCIPAL="hive/presto-master.docker.cluster@OTHERREALM.COM" and KEYTAB = "${keytab_root_dir}/other-hive-presto-master.keytab";"""
        //     rowNum beNum
        // }

        // sql """drop catalog ${hms_catalog_name};"""
        // sql """drop catalog other_${hms_catalog_name};"""
    }
}
