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

suite("test_export_kerberos", "p0,external,kerberos,external_docker,external_docker_kerberos") {
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
                "hive.metastore.kerberos.principal" = "hive/hadoop-master@LABS.TERADATA.COM"
            );
        """

    sql """ switch ${hms_catalog_name} """
    sql """ show databases """
    sql """ drop database test_export_out_file if exist"""
    sql """ create database test_export_out_file """
    sql """ use test_export_out_file """
    sql """
        CREATE TABLE `test_export_out_file` (
          `c_tinyint` tinyint(4) NULL COMMENT "",
          `c_smallint` smallint(6) NULL COMMENT ""
        ) ;
    """
    sql """
        insert into test_export_out_file values(1,2);
    """
    def export_result = sql """
        EXPORT TABLE test_export_out_file
        TO "hdfs://${externalEnvIp}:8520/user/to/export_" 
        PROPERTIES
        (
            "line_delimiter" = ","
        )
        with HDFS (
            "fs.defaultFS"="hdfs://${externalEnvIp}:8520",
            "hadoop.security.authentication" = "kerberos",
            "hadoop.security.auth_to_local" = "RULE:[2:\$1@\$0](.*@LABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\$1@\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\$1@\$0](.*@OTHERREALM.COM)s/@.*//
                                   DEFAULT",
                "hadoop.kerberos.min.seconds.before.relogin" = "5",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab",
        );
       """
    println(export_result)
    def outfile_result=sql """
        SELECT * FROM test_krb_hive_tbl
        INTO OUTFILE "hdfs://${externalEnvIp}:8520/user/to/outfile_"
         FORMAT AS CSV
        PROPERTIES(
         "fs.defaultFS"="hdfs://${externalEnvIp}:8520",
                
                    "hadoop.security.auth_to_local" = "RULE:[2:\\\$1@\\\$0](.*@LABS.TERADATA.COM)s/@.*//
                                           RULE:[2:\\\$1@\\\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                           RULE:[2:\\\$1@\\\$0](.*@OTHERREALM.COM)s/@.*//
                                           DEFAULT",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab",
        )
        """
    println(outfile_result)
    def export_status= sql """
       show export 
    """
    println(export_status)
    

}
