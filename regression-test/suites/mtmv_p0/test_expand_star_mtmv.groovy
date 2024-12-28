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

import org.junit.Assert;

suite("test_expand_star_mtmv","mtmv") {
    String suiteName = "test_expand_star_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String functionName = "${suiteName}_function"
    String dbName = context.config.getDbNameByFile(context.file)

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 INT,
            k3 varchar(32)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'version_info'='3'
        )
        AS
        SELECT * from ${tableName};
        """

    order_qt_query "SELECT QuerySql FROM mv_infos('database'='${dbName}') where Name = '${mvName}'"

    sql """drop materialized view if exists ${mvName};"""

    def jarPath = """${context.config.suitePath}/javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)
    log.info("jarPath:${jarPath}")

    sql "drop function if exists ${functionName}(date, date)"
    sql """ CREATE FUNCTION ${functionName}(date, date) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTest1",
            "type"="JAVA_UDF"
        ); """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'version_info'='3'
        )
        AS
        SELECT ${functionName} ('2011-01-01','2011-01-03') as k1 from ${tableName};
        """
    order_qt_udf "SELECT QuerySql FROM mv_infos('database'='${dbName}') where Name = '${mvName}'"
    sql "drop function if exists ${functionName}(date, date)"
    sql """drop materialized view if exists ${mvName};"""
}
