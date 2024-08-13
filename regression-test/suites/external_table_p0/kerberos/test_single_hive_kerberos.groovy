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
    String enabled = context.config.otherConfigs.get("enableKerberosTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_catalog_name = "test_single_hive_kerberos"
        sql """drop catalog if exists hms_kerberos;"""
        sql """
            CREATE CATALOG IF NOT EXISTS hms_kerberos
            PROPERTIES (
                "type" = "hms",
                "hive.metastore.uris" = "thrift://172.31.71.25:9083",
                "fs.defaultFS" = "hdfs://172.31.71.25:8020",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="presto-server/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "/keytabs/presto-server.keytab",
                "hive.metastore.sasl.enabled " = "true",
                "hive.metastore.kerberos.principal" = "hive/_HOST@LABS.TERADATA.COM"
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
                    "hive.metastore.uris" = "thrift://172.31.71.25:9083",
                    "fs.defaultFS" = "hdfs://172.31.71.25:8020",
                    "hadoop.security.authentication" = "kerberos",
                    "hadoop.kerberos.principal"="presto-server/presto-master.docker.cluster@LABS.TERADATA.COM",
                    "hadoop.kerberos.keytab" = "/keytabs/presto-server.keytab"
                );
            """
            sql """ switch hms_kerberos_hadoop_err1 """
            sql """ show databases """
        } catch (Exception e) {
            logger.info(e.toString())
            // caused by a warning msg if enable sasl on hive but "hive.metastore.sasl.enabled" is not true:
            // "set_ugi() not successful, Likely cause: new client talking to old server. Continuing without it."
            assertTrue(e.toString().contains("org.apache.thrift.transport.TTransportException: null"))
        }

        try {
            sql """drop catalog if exists hms_kerberos_hadoop_err2;"""
            sql """
                CREATE CATALOG IF NOT EXISTS hms_kerberos_hadoop_err2
                PROPERTIES (
                    "type" = "hms",
                    "hive.metastore.sasl.enabled " = "true",
                    "hive.metastore.uris" = "thrift://172.31.71.25:9083",
                    "fs.defaultFS" = "hdfs://172.31.71.25:8020"
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
        //                    "hive.metastore.uris" = "thrift://172.31.71.25:9083",
        //                    "fs.defaultFS" = "hdfs://172.31.71.25:8020",
        //                    "hadoop.security.authentication" = "kerberos",
        //                    "hadoop.kerberos.principal"="presto-server/presto-master.docker.cluster@LABS.TERADATA.COM",
        //                    "hadoop.kerberos.keytab" = "/keytabs/presto-server.keytab",
        //                    "hive.metastore.thrift.impersonation.enabled" = true"
        //                    "hive.metastore.client.credential-cache.location" = "hive-presto-master-krbcc"
        //                );
        //            """
        //            sql """ switch hms_keberos_ccache """
        //            sql """ show databases """
        //        } catch (Exception e) {
        //            logger.error(e.message)
        //        }
    }
}
