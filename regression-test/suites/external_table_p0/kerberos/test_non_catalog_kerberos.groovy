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

import org.awaitility.Awaitility;
import static java.util.concurrent.TimeUnit.SECONDS;

suite("test_non_catalog_kerberos", "p0,external,kerberos,external_docker,external_docker_kerberos") {
    String enabled = context.config.otherConfigs.get("enableNonCatalogKerberosTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    def String hms_catalog_name = "hms_catalog_kerberos_test_export"
    def String test_tbl_name="hms_test_table"
    def keytab_root_dir = "/keytabs"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """
            drop catalog if exists ${hms_catalog_name}
        """
    sql """
            CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
            PROPERTIES ( 
            "type" = "hms",
            "ipc.client.fallback-to-simple-auth-allowed" = "true",
            "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
            "hive.metastore.sasl.enabled " = "true",
            "hive.metastore.kerberos.principal" = "hive/hadoop-master@LABS.TERADATA.COM",
            "hadoop.security.authentication" = "kerberos",
            "hadoop.security.auth_to_local" = "RULE:[2:\$1@\$0](.*@LABS.TERADATA.COM)s/@.*//
                                               RULE:[2:\$1@\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                               RULE:[2:\$1@\$0](.*@OTHERREALM.COM)s/@.*//
                                               DEFAULT",
            "hadoop.kerberos.principal" = "hive/presto-master.docker.cluster@LABS.TERADATA.COM",
            "hadoop.kerberos.min.seconds.before.relogin" = "5",
            "hadoop.kerberos.keytab.login.autorenewal.enabled" = "false",
              "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab",
            "fs.defaultFS" = "hdfs://${externalEnvIp}:8520"
            ); 
        """

    sql """ switch ${hms_catalog_name} """
    sql """ use test_krb_hive_db """
    sql """ drop table  if exists ${test_tbl_name}"""
    sql """
        CREATE TABLE `${test_tbl_name}` (
          `c_tinyint` tinyint(4) NULL COMMENT "",
          `c_smallint` smallint(6) NULL COMMENT ""
        ) ;
    """
    sql """
        insert into ${test_tbl_name} values(1,2);
    """
    qt_select1 "select * from ${test_tbl_name} "
    def export_task_label= "export_kerberos_test"+ System.currentTimeMillis()

    sql """
        EXPORT TABLE ${test_tbl_name}
        TO "hdfs://${externalEnvIp}:8520/user/test/export_" 
        PROPERTIES
        (
            "line_delimiter" = ",",
            "label"="${export_task_label}"
        )
        with HDFS (
          "fs.defaultFS" = "hdfs://${externalEnvIp}:8520",
            "hadoop.security.auth_to_local" = "RULE:[2:\\\$1@\\\$0](.*@LABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERREALM.COM)s/@.*//
                                   DEFAULT",
                "hadoop.kerberos.min.seconds.before.relogin" = "5",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.keytab.login.autorenewal.enabled"="false",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab"
        );
       """

    def outfile_result=sql """
        SELECT * FROM ${test_tbl_name}
        INTO OUTFILE "hdfs://${externalEnvIp}:8520/user/to/outfile_"
         FORMAT AS CSV
        PROPERTIES(
            "fs.defaultFS" = "hdfs://${externalEnvIp}:8520",
            "hadoop.security.auth_to_local" = "RULE:[2:\\\$1@\\\$0](.*@LABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERREALM.COM)s/@.*//
                                   DEFAULT",
                "hadoop.kerberos.min.seconds.before.relogin" = "5",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.keytab.login.autorenewal.enabled"="false",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                  "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab"
        )
        """

    println(outfile_result)
    def hdfslink=outfile_result.get(0).get(3)
    println hdfslink
    qt_select1 """
      select * from hdfs(
            "uri" = "${hdfslink}",
            "hadoop.username" = "doris",
            "format" = "csv",
           "fs.defaultFS" = "hdfs://${externalEnvIp}:8520",
            "hadoop.security.auth_to_local" = "RULE:[2:\\\$1@\\\$0](.*@LABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERREALM.COM)s/@.*//
                                   DEFAULT",
                "hadoop.kerberos.min.seconds.before.relogin" = "5",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.keytab.login.autorenewal.enabled"="false",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab"
                )

    """
    Awaitility.await("queery-export-task-result-test").atMost(60, SECONDS).pollInterval(5, SECONDS).until(
            {
                sql """ switch ${hms_catalog_name} """
                sql """ use test_krb_hive_db """
                def res = sql """ show export where label = "${export_task_label}" """
                if (res[0][2] == "FINISHED") {
                    return true
                } else if (res[0][2] == "CANCELLED") {
                    throw new IllegalStateException("""export failed: ${res[0][10]}""")
                } else {
                    return false
                }
            }
    )


}
