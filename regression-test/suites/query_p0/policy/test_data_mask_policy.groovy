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

suite("test_data_mask_policy", "query,p0") {

    def dbName = "test_data_mask_db"
    def tableName = "test_data_mask_tbl"
    def userName = "jack"

    sql "create database if not exists ${dbName}"

    sql "use ${dbName}"

    sql "drop table if exists `${tableName}`"

    sql """
        CREATE TABLE `${tableName}` (
          `k1` int NULL,
          `k2` datetime NULL,
          `k3` varchar(20) NULL,
          `k4` varchar(20) NULL,
          `k5` varchar(20) NULL,
          `k6` varchar(20) NULL,
          `k7` varchar(20) NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
           "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "insert into ${tableName} values(1,'2022-01-01 12:13:14','abcdef','abcdef','abcdef','aBc4E121','abc')"

    sql "create user if not exists ${userName}"
    sql "grant SELECT_PRIV on ${dbName}.${tableName} to '${userName}'@'%';"

    sql "drop data mask policy if exists test"
    sql "drop data mask policy if exists test1"
    sql "drop data mask policy if exists test2"
    sql "drop data mask policy if exists test3"
    sql "drop data mask policy if exists test4"
    sql "drop data mask policy if exists test5"
    sql "drop data mask policy if exists test6"

    sql "create data mask policy test on internal.${dbName}.${tableName}.k1 to 'jack' using MASK_DEFAULT"

    sql "create data mask policy test1 on internal.${dbName}.${tableName}.k2 to 'jack' using MASK_DATE_SHOW_YEAR"

    sql "create data mask policy test2 on internal.${dbName}.${tableName}.k3 to 'jack' using MASK_SHOW_LAST_4"

    sql "create data mask policy test3 on internal.${dbName}.${tableName}.k4 to 'jack' using MASK_SHOW_FIRST_4"

    sql "create data mask policy test4 on internal.${dbName}.${tableName}.k5 to 'jack' using MASK_HASH"

    sql "create data mask policy test5 on internal.${dbName}.${tableName}.k6 to 'jack' using MASK_REDACT"

    sql "create data mask policy test6 on internal.${dbName}.${tableName}.k7 to 'jack' using MASK_NULL"

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${userName}""";
    }

    def defaultDbUrl = context.config.jdbcUrl.substring(0, context.config.jdbcUrl.lastIndexOf("/"))
    logger.info("connect to ${defaultDbUrl}".toString())
    connect(user = userName, password = null, url = defaultDbUrl) {
        qt_sql_1 "select * from ${dbName}.${tableName}"
    }

    sql "drop data mask policy if exists test"
    sql "drop data mask policy if exists test1"
    sql "drop data mask policy if exists test2"
    sql "drop data mask policy if exists test3"
    sql "drop data mask policy if exists test4"
    sql "drop data mask policy if exists test5"
    sql "drop data mask policy if exists test6"
    sql "drop table if exists ${tableName}"
    sql "drop database if exists ${dbName}"
}
