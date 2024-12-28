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

suite("test_catalog_upgrade_load", "p0,external,hive,external_docker,external_docker_hive,restart_fe,upgrade_case") {

    // Hive
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hivePrefix = "hive2"
        String catalog_name = "test_catalog_upgrade_hive2"
        String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
        String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
    }

    // Iceberg rest catalog
    enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalog_name = "test_catalog_upgrade_iceberg_rest"

        sql """drop catalog if exists ${catalog_name}"""
        sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${rest_port}',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.region" = "us-east-1"
        );"""
    }

    // Iceberg hms catalog
    enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hivePrefix = "hive2"
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalog_name = "test_catalog_upgrade_iceberg_hms"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'warehouse' = 'hdfs://${externalEnvIp}:${hdfs_port}/user/iceberg_test/',
            'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
        );""" 
    }

    // Paimon filesystem catalog
    enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalog_name = "test_catalog_upgrade_paimon_fs"
        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type" = "paimon",
            "paimon.catalog.type"="filesystem",
            "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
        );""" 
    }

    // Kerberos hive catalog
    enabled = context.config.otherConfigs.get("enableKerberosTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "test_catalog_upgrade_kerberos_hive"
        // sql """drop catalog if exists ${catalog_name};"""
        // sql """
        //     CREATE CATALOG IF NOT EXISTS ${catalog_name}
        //     PROPERTIES (
        //         "type" = "hms",
        //         "hive.metastore.uris" = "thrift://172.31.71.25:9083",
        //         "fs.defaultFS" = "hdfs://172.31.71.25:8020",
        //         "hadoop.kerberos.min.seconds.before.relogin" = "5",
        //         "hadoop.security.authentication" = "kerberos",
        //         "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
        //         "hadoop.kerberos.keytab" = "/keytabs/hive-presto-master.keytab",
        //         "hive.metastore.sasl.enabled " = "true",
        //         "hive.metastore.kerberos.principal" = "hive/_HOST@LABS.TERADATA.COM"
        //     );
        // """

        // sql """drop catalog if exists other_${catalog_name};"""
        // sql """
        //     CREATE CATALOG IF NOT EXISTS other_${catalog_name}
        //     PROPERTIES (
        //         "type" = "hms",
        //         "hive.metastore.uris" = "thrift://172.31.71.26:9083",
        //         "fs.defaultFS" = "hdfs://172.31.71.26:8020",
        //         "hadoop.kerberos.min.seconds.before.relogin" = "5",
        //         "hadoop.security.authentication" = "kerberos",
        //         "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@OTHERREALM.COM",
        //         "hadoop.kerberos.keytab" = "/keytabs/other-hive-presto-master.keytab",
        //         "hive.metastore.sasl.enabled " = "true",
        //         "hive.metastore.kerberos.principal" = "hive/_HOST@OTHERREALM.COM",
        //         "hadoop.security.auth_to_local" ="RULE:[2:\$1@\$0](.*@OTHERREALM.COM)s/@.*//
        //                                           RULE:[2:\$1@\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
        //                                           DEFAULT"
        //     );
        // """
    }

    // Jdbc MySQL catalog
    enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
        // String driver_url = "mysql-connector-java-8.0.25.jar"
        String catalog_name = "test_catalog_upgrade_jdbc_mysql"
        sql """drop catalog if exists ${catalog_name} """
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""
    }
}

