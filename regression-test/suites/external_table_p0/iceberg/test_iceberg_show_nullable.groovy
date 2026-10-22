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

suite("test_iceberg_show_nullable", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_show_nullable"

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

    sql """switch ${catalog_name}"""
    sql """drop database if exists test_iceberg_show_nullable_db force"""
    sql """create database test_iceberg_show_nullable_db"""
    sql """use test_iceberg_show_nullable_db"""

    sql """
    create table iceberg_show_nullable (
        id bigint not null,
        value string not null comment 'required value',
        event_time datetime
    )"""
    String columnsQuery = """select COLUMN_NAME, IS_NULLABLE from information_schema.columns
        where TABLE_SCHEMA = 'test_iceberg_show_nullable_db' and TABLE_NAME = 'iceberg_show_nullable'"""

    // A new table has no snapshot yet, but its schema already declares the required columns.
    qt_snapshots_empty """select count(*) from iceberg_show_nullable\$snapshots"""
    qt_desc_empty """desc iceberg_show_nullable"""
    order_qt_information_schema_empty(columnsQuery)

    sql """insert into iceberg_show_nullable values (1, 'a', '2026-01-01 00:00:00'), (2, 'b', null)"""

    qt_desc """desc iceberg_show_nullable"""
    qt_show_columns """show columns from iceberg_show_nullable"""
    qt_show_full_columns """show full columns from iceberg_show_nullable"""
    qt_show_columns_like """show columns from iceberg_show_nullable like 'id'"""
    // SHOW COLUMNS ... WHERE is rewritten to information_schema.columns; no ORDER BY, so the rows are
    // sorted by the framework (order_qt_) instead of being compared in an unspecified scan order.
    order_qt_show_columns_where """show columns from iceberg_show_nullable where `Null` = 'NO'"""
    order_qt_show_full_columns_where """show full columns from iceberg_show_nullable where `Null` = 'YES'"""
    order_qt_information_schema(columnsQuery)
    order_qt_information_schema_required(columnsQuery + " and IS_NULLABLE = 'NO'")
    String ddl = sql("""show create table iceberg_show_nullable""")[0][1]
    assertTrue(ddl.contains("`id` bigint NOT NULL"), ddl)
    assertTrue(ddl.contains("`value` text NOT NULL COMMENT \"required value\""), ddl)
    assertTrue(ddl.contains("`event_time` datetimev2(6) NULL"), ddl)
    order_qt_select """select * from iceberg_show_nullable"""

    // Only the metadata view is NOT NULL; the columns are read as nullable, and a write still enforces
    // the declared constraint.
    test {
        sql """insert into iceberg_show_nullable values (null, 'bad', '2026-01-02 00:00:00')"""
        exception "Column 'id' is declared non-nullable but contains nulls"
    }
    test {
        sql """insert into iceberg_show_nullable values (3, null, '2026-01-02 00:00:00')"""
        exception "Column 'value' is declared non-nullable but contains nulls"
    }

    // Relaxing the constraint is reflected without a new data snapshot.
    qt_snapshots_before_alter """select count(*) from iceberg_show_nullable\$snapshots"""
    sql """alter table iceberg_show_nullable modify column value string null"""
    qt_snapshots_after_alter """select count(*) from iceberg_show_nullable\$snapshots"""
    qt_desc_after_alter """desc iceberg_show_nullable"""
    order_qt_information_schema_after_alter(columnsQuery)
    ddl = sql("""show create table iceberg_show_nullable""")[0][1]
    assertTrue(ddl.contains("`value` text NULL COMMENT \"required value\""), ddl)
    sql """insert into iceberg_show_nullable values (3, null, null)"""
    order_qt_select_after_alter """select * from iceberg_show_nullable"""
    test {
        sql """insert into iceberg_show_nullable values (null, 'still required', null)"""
        exception "Column 'id' is declared non-nullable but contains nulls"
    }
}
