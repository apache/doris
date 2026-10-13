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

// A JDBC catalog that points at another Doris keeps the remote VARBINARY columns as VARBINARY with their
// declared length, like the binary types of every other JDBC source; the plugin connector used to map them to
// STRING. A remote Doris exposes VARBINARY through views and external tables, so the remote side is a view.
suite("test_doris_jdbc_catalog_varbinary", "p0,external") {
    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    String catalog_name = "test_doris_jdbc_catalog_varbinary"
    String internal_db_name = "regression_test_jdbc_catalog_varbinary"

    sql """create database if not exists ${internal_db_name}"""
    sql """drop view if exists ${internal_db_name}.varbinary_view"""
    sql """
        create view ${internal_db_name}.varbinary_view as
        select 1 as id, cast('abc' as varbinary(16)) as b
        union all
        select 2, cast(null as varbinary(16))
    """
    order_qt_internal """select id, b from internal.${internal_db_name}.varbinary_view order by id"""
    sql """drop view if exists ${internal_db_name}.varbinary_array_view"""
    sql """
        create view ${internal_db_name}.varbinary_array_view as
        select 1 as id, array(cast('abc' as varbinary(16)), cast(null as varbinary(16)), cast('' as varbinary(16))) as a
        union all
        select 2, cast(null as array<varbinary(16)>)
    """

    sql """drop catalog if exists ${catalog_name}"""
    sql """CREATE CATALOG `${catalog_name}` PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
    )"""

    // The remote VARBINARY column stays VARBINARY instead of becoming STRING; its length is the one the remote
    // reports through the JDBC metadata.
    order_qt_desc """desc ${catalog_name}.${internal_db_name}.varbinary_view"""
    // The bytes read through the catalog are the remote ones.
    order_qt_select """select id, b, hex(b) from ${catalog_name}.${internal_db_name}.varbinary_view order by id"""

    // The remote returns an array as text that spells each VARBINARY element as 0x hex, and the scanner decodes
    // array elements only into strings, so a VARBINARY element stays STRING and reads as that text.
    order_qt_array_desc """desc ${catalog_name}.${internal_db_name}.varbinary_array_view"""
    order_qt_array_select """select id, a, a[1] from ${catalog_name}.${internal_db_name}.varbinary_array_view order by id"""
}
