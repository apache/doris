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

suite("test_mc_join", "p2,external,maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    boolean hiveEnabled = "true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))
    boolean icebergEnabled = "true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mcCatalog = "test_mc_join_mc"
    String otherMcCatalog = "test_mc_join_mc_other"
    String internalDb = "test_mc_join_internal_db"
    String internalTb = "test_mc_join_internal_tbl"

    sql """drop catalog if exists ${mcCatalog}"""
    sql """drop catalog if exists ${otherMcCatalog}"""
    sql """
        create catalog if not exists ${mcCatalog} properties (
            "type" = "max_compute",
            "mc.default.project" = "mc_datalake_schema",
            "mc.access_key" = "${ak}",
            "mc.secret_key" = "${sk}",
            "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
            "mc.enable.namespace.schema" = "true"
        );
    """
    sql """
        create catalog if not exists ${otherMcCatalog} properties (
            "type" = "max_compute",
            "mc.default.project" = "other_mc_datalake_test",
            "mc.access_key" = "${ak}",
            "mc.secret_key" = "${sk}",
            "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api"
        );
    """

    sql """create database if not exists internal.${internalDb}"""
    sql """drop table if exists internal.${internalDb}.${internalTb}"""
    sql """
        create table internal.${internalDb}.${internalTb} (
            id int,
            dept string
        )
        distributed by hash(id) buckets 1
        properties ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        insert into internal.${internalDb}.${internalTb} values
            (1, 'finance'),
            (2, 'it'),
            (4, 'ops')
    """

    // J-01: Join a MaxCompute table with an internal OLAP table.
    order_qt_j01_mc_join_internal """
        select u.id, u.name, i.dept
        from ${mcCatalog}.`default`.user_info u
        join internal.${internalDb}.${internalTb} i
          on u.id = i.id
        order by u.id
    """

    if (hiveEnabled) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get("hive3HmsPort")
        String hdfsPort = context.config.otherConfigs.get("hive3HdfsPort")
        String hiveCatalog = "test_mc_join_external_hive"
        String hiveDb = "test_mc_join_hive_db"
        String hiveTb = "test_mc_join_hive_tbl"

        sql """drop catalog if exists ${hiveCatalog}"""
        sql """
            create catalog if not exists ${hiveCatalog} properties (
                "type" = "hms",
                "hive.metastore.uris" = "thrift://${externalEnvIp}:${hmsPort}",
                "fs.defaultFS" = "hdfs://${externalEnvIp}:${hdfsPort}"
            );
        """

        sql """switch ${hiveCatalog}"""
        sql """create database if not exists ${hiveDb}"""
        sql """use ${hiveDb}"""
        sql """drop table if exists ${hiveTb}"""
        sql """
            create table ${hiveTb} (
                id int,
                city string,
                segment string
            )
        """
        sql """
            insert into ${hiveTb} values
                (1, 'Beijing', 'north'),
                (2, 'Shanghai', 'east'),
                (8, 'Hangzhou', 'west')
        """

        // J-02: Join a Hive table with a MaxCompute table.
        order_qt_j02_mc_join_hive """
            select u.id, u.name, h.segment
            from ${mcCatalog}.`default`.user_info u
            join ${hiveCatalog}.${hiveDb}.${hiveTb} h
              on u.id = h.id and u.city = h.city
            order by u.id
        """
    } else {
        logger.info("skip Hive join case because enableHiveTest is false")
    }

    if (icebergEnabled) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String icebergCatalog = "test_mc_join_external_iceberg"
        String icebergDb = "test_mc_join_iceberg_db"
        String icebergTb = "test_mc_join_iceberg_tbl"

        sql """drop catalog if exists ${icebergCatalog}"""
        sql """
            create catalog if not exists ${icebergCatalog} properties (
                "type" = "iceberg",
                "iceberg.catalog.type" = "rest",
                "uri" = "http://${externalEnvIp}:${rest_port}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1"
            );
        """

        sql """switch ${icebergCatalog}"""
        sql """create database if not exists ${icebergDb}"""
        sql """use ${icebergDb}"""
        sql """drop table if exists ${icebergTb}"""
        sql """create table ${icebergTb} (id int, tier string)"""
        sql """
            insert into ${icebergTb} values
                (1, 'gold'),
                (3, 'silver'),
                (9, 'bronze')
        """

        // J-03: Join an Iceberg table with a MaxCompute table.
        order_qt_j02_mc_join_iceberg """
            select u.id, u.name, i.tier
            from ${mcCatalog}.`default`.user_info u
            join ${icebergCatalog}.${icebergDb}.${icebergTb} i
              on u.id = i.id
            order by u.id
        """
    } else {
        logger.info("skip Iceberg join case because enableIcebergTest is false")
    }

    // J-04: Join tables across two MaxCompute catalogs from different projects.
    order_qt_j03_mc_join_cross_catalog """
        select u.id, u.name, o.col
        from ${mcCatalog}.`default`.user_info u
        join ${otherMcCatalog}.other_mc_datalake_test.other_db_mc_tb o
          on u.id = o.id
        order by u.id
    """
}
