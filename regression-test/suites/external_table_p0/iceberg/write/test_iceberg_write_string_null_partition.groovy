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

suite("test_iceberg_write_string_null_partition",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists iceberg_string_null_partition_catalog"""
    sql """
        create catalog iceberg_string_null_partition_catalog properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        )
    """
    sql """switch iceberg_string_null_partition_catalog"""
    sql """create database if not exists iceberg_string_null_partition_db"""
    sql """use iceberg_string_null_partition_db"""

    for (String fileFormat : ["parquet", "orc"]) {
        sql """drop table if exists iceberg_string_null_partition"""
        sql """
            create table iceberg_string_null_partition (id int, p string)
            partition by list (p) ()
            properties ("format-version" = "2", "write.format.default" = "${fileFormat}")
        """
        sql """
            insert into iceberg_string_null_partition values
                (1, 'null'), (2, NULL), (3, 'ordinary'), (4, ''), (5, 'NULL'),
                (6, '__HIVE_DEFAULT_PARTITION__')
        """
        "order_qt_${fileFormat}_rows" """
            select id, p, p is null from iceberg_string_null_partition order by id
        """
        "order_qt_${fileFormat}_literal_null" """
            select id from iceberg_string_null_partition where p = 'null' order by id
        """
        "order_qt_${fileFormat}_sql_null" """
            select id from iceberg_string_null_partition where p is null order by id
        """
        "order_qt_${fileFormat}_empty" """
            select id from iceberg_string_null_partition where p = '' order by id
        """
        "order_qt_${fileFormat}_partitions" """
            select cast(`partition` as string), sum(record_count)
            from iceberg_string_null_partition\$files
            group by cast(`partition` as string) order by cast(`partition` as string)
        """

        sql """insert overwrite table iceberg_string_null_partition partition(p='null') select 10"""
        sql """insert overwrite table iceberg_string_null_partition partition(p=NULL) select 20"""
        "order_qt_${fileFormat}_static_rows" """
            select id, p, p is null from iceberg_string_null_partition order by id
        """
        "order_qt_${fileFormat}_static_literal_null" """
            select id from iceberg_string_null_partition where p = 'null' order by id
        """
        "order_qt_${fileFormat}_static_sql_null" """
            select id from iceberg_string_null_partition where p is null order by id
        """

        sql """drop table if exists iceberg_string_null_hybrid"""
        sql """
            create table iceberg_string_null_hybrid (id int, p string, q string)
            partition by list (p, q) ()
            properties ("format-version" = "2", "write.format.default" = "${fileFormat}")
        """
        sql """insert into iceberg_string_null_hybrid values
            (1, 'null', 'a'), (2, NULL, 'a'), (3, 'ordinary', 'a')"""
        sql """insert overwrite table iceberg_string_null_hybrid partition(p='null') select 10, 'b'"""
        sql """insert overwrite table iceberg_string_null_hybrid partition(p=NULL) select 20, 'b'"""
        "order_qt_${fileFormat}_hybrid_rows" """
            select id, p, q, p is null from iceberg_string_null_hybrid order by id
        """
        "order_qt_${fileFormat}_hybrid_literal_null" """
            select id from iceberg_string_null_hybrid where p = 'null' order by id
        """
        "order_qt_${fileFormat}_hybrid_sql_null" """
            select id from iceberg_string_null_hybrid where p is null order by id
        """

        sql """drop table if exists iceberg_string_null_truncate"""
        sql """
            create table iceberg_string_null_truncate (id int, p string)
            partition by list (truncate(4, p)) ()
            properties ("format-version" = "2", "write.format.default" = "${fileFormat}")
        """
        sql """insert into iceberg_string_null_truncate values
            (1, 'null-value'), (2, NULL), (3, 'ordinary')"""
        "order_qt_${fileFormat}_truncate_rows" """
            select id, p from iceberg_string_null_truncate order by id
        """
        "order_qt_${fileFormat}_truncate_literal_null" """
            select id from iceberg_string_null_truncate where p = 'null-value' order by id
        """
        "order_qt_${fileFormat}_truncate_sql_null" """
            select id from iceberg_string_null_truncate where p is null order by id
        """
    }
}
