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

suite("test_single_hive_kerberos", "p0,external,kerberos,external_docker,external_docker_kerberos") {
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
        String hms_catalog_name = "test_single_hive_kerberos"
        sql """drop catalog if exists hms_kerberos;"""
        sql """
            CREATE CATALOG IF NOT EXISTS hms_kerberos
            PROPERTIES (
                "type" = "hms",
                "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
                "fs.defaultFS" = "hdfs://${externalEnvIp}:8520",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="presto-server/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/presto-server.keytab",
                "hadoop.security.auth_to_local" = "RULE:[2:\$1@\$0](.*@LABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\$1@\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\$1@\$0](.*@OTHERREALM.COM)s/@.*//
                                   DEFAULT",
                "hive.metastore.sasl.enabled " = "true",
                "hive.metastore.kerberos.principal" = "hive/hadoop-master@LABS.TERADATA.COM"
            );
        """
        sql """ switch hms_kerberos """
        sql """ show databases """
        order_qt_q01 """ select * from hms_kerberos.test_krb_hive_db.test_krb_hive_tbl """
        sql """drop catalog hms_kerberos;"""

        try {
            sql """drop catalog if exists hms_kerberos_hadoop_err1;"""
            sql """
                CREATE CATALOG IF NOT EXISTS hms_kerberos_hadoop_err1
                PROPERTIES (
                    "type" = "hms",
                    "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
                    "fs.defaultFS" = "hdfs://${externalEnvIp}:8520",
                    "hadoop.security.authentication" = "kerberos",
                    "hadoop.kerberos.principal"="presto-server/presto-master.docker.cluster@LABS.TERADATA.COM",
                    "hadoop.kerberos.keytab" = "${keytab_root_dir}/presto-server.keytab"
                );
            """
            sql """ switch hms_kerberos_hadoop_err1 """
            sql """ show databases """
        } catch (Exception e) {
            logger.info(e.toString())
            // caused by a warning msg if enable sasl on hive but "hive.metastore.sasl.enabled" is not true:
            // "set_ugi() not successful, Likely cause: new client talking to old server. Continuing without it."
            assertTrue(e.toString().contains("thrift.transport.TTransportException"))
        }

        try {
            sql """drop catalog if exists hms_kerberos_hadoop_err2;"""
            sql """
                CREATE CATALOG IF NOT EXISTS hms_kerberos_hadoop_err2
                PROPERTIES (
                    "type" = "hms",
                    "hive.metastore.sasl.enabled " = "true",
                    "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
                    "fs.defaultFS" = "hdfs://${externalEnvIp}:8520"
                );
            """
            sql """ switch hms_kerberos_hadoop_err2 """
            sql """ show databases """
        } catch (Exception e) {
            // org.apache.thrift.transport.TTransportException: GSS initiate failed
            assertTrue(e.toString().contains("Could not connect to meta store using any of the URIs provided. Most recent failure: shade.doris.hive.org.apache.thrift.transport.TTransportException: GSS initiate failed"))
        }

        //        try {
        //            sql """
        //                CREATE CATALOG IF NOT EXISTS hms_keberos_ccache
        //                PROPERTIES (
        //                    "type" = "hms",
        //                    "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
        //                    "fs.defaultFS" = "hdfs://${externalEnvIp}:8520",
        //                    "hadoop.security.authentication" = "kerberos",
        //                    "hadoop.kerberos.principal"="presto-server/presto-master.docker.cluster@LABS.TERADATA.COM",
        //                    "hadoop.kerberos.keytab" = "${keytab_root_dir}/presto-server.keytab",
        //                    "hive.metastore.thrift.impersonation.enabled" = true"
        //                    "hive.metastore.client.credential-cache.location" = "hive-presto-master-krbcc"
        //                );
        //            """
        //            sql """ switch hms_keberos_ccache """
        //            sql """ show databases """
        //        } catch (Exception e) {
        //            logger.error(e.message)
        //        }
        
        // test information_schema.backend_kerberos_ticket_cache
        // switch to a normal catalog
        // sql "switch internal";
        // List<List<Object>> backends = sql "show backends"
        // int beNum = backends.size();
        // test {
        //     sql """select * from information_schema.backend_kerberos_ticket_cache where PRINCIPAL="presto-server/presto-master.docker.cluster@LABS.TERADATA.COM" and KEYTAB = "${keytab_root_dir}/presto-server.keytab";"""
        //     rowNum beNum
        // } 
    }
}
