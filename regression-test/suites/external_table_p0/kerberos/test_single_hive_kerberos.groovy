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
        sql """drop catalog if exists ${hms_catalog_name};"""
        sql """
            CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
            PROPERTIES (
                "type" = "hms",
                "hive.metastore.uris" = "thrift://172.20.70.25:9083",
                "fs.defaultFS" = "hdfs://172.20.70.25:8020",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="presto-server/trino-worker.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "presto-server.keytab",
                "hive.metastore.sasl.enabled " = "true",
                "hive.metastore.kerberos.principal" = "hive/_HOST@LABS.TERADATA.COM"
            );
        """
        logger.info("switched to catalog " + hms_catalog_name)
        sql """ show databases """
        sql """ use test_krb_hive_db """
        sql """ select * from test_krb_hive_db.test_krb_hive_tbl """
        sql """drop catalog ${hms_catalog_name};"""

        try {
            sql """
                CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
                PROPERTIES (
                    "type" = "hms",
                    "hive.metastore.uris" = "thrift://hadoop-master:9083",
                    "fs.defaultFS" = "hdfs://hadoop-master:8020",
                    "hadoop.security.authentication" = "kerberos",
                    "hadoop.kerberos.principal"="presto-server/trino-worker.docker.cluster@LABS.TERADATA.COM",
                    "hadoop.kerberos.keytab" = "presto-server.keytab"
                );
            """
        } catch (Exception e) {
            logger.error(e.contains("new client talking to old server. Continuing without it."))
        }

        try {
            sql """
                CREATE CATALOG IF NOT EXISTS hms_keberos_ccache
                PROPERTIES (
                    "type" = "hms",
                    "hive.metastore.uris" = "thrift://172.20.70.25:9083",
                    "fs.defaultFS" = "hdfs://172.20.70.25:8020",
                    "hadoop.security.authentication" = "kerberos",
                    "hadoop.kerberos.principal"="presto-server/trino-worker.docker.cluster@LABS.TERADATA.COM",
                    "hadoop.kerberos.keytab" = "presto-server.keytab",
                    "hive.metastore.thrift.impersonation.enabled" = true"
                    "hive.metastore.client.credential-cache.location" = "hive-presto-master-krbcc"
                );
            """
        } catch (Exception e) {
            logger.error(e.message)
        }
    }
}
