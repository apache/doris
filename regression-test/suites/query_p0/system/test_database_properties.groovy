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

suite("test_database_properties") {
    def dbName = "test_database_properties_db"
    sql "drop database if exists ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "use ${dbName}"

    sql """
        ALTER DATABASE ${dbName} SET PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "3600"
        );
    """

    def prop_count = sql """
        select count(*) from information_schema.database_properties
        where schema_name = "${dbName}" and property_name in ("replication_allocation","binlog.enable","binlog.ttl_seconds")
    """;
    assert prop_count.first()[0] == 3;

    qt_select_check_1 """
        select * from information_schema.database_properties
        where schema_name = "${dbName}"
        order by CATALOG_NAME, SCHEMA_NAME, PROPERTY_NAME, PROPERTY_VALUE
    """

    sql """
        ALTER DATABASE ${dbName} SET PROPERTIES (
            "replication_allocation" = "tag.location.default:1"
        );
    """
    qt_select_check_2 """
        select * from information_schema.database_properties
        where schema_name = "${dbName}"
        order by CATALOG_NAME, SCHEMA_NAME, PROPERTY_NAME, PROPERTY_VALUE
    """

    def user = "database_properties_user"
    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '123abc!@#'"

    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    sql "GRANT SELECT_PRIV ON information_schema.database_properties TO ${user}"

    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"
    connect(user, '123abc!@#', url) {
        qt_select_check_3 """
            select * from information_schema.database_properties
            order by CATALOG_NAME, SCHEMA_NAME, PROPERTY_NAME, PROPERTY_VALUE
        """
    }

    sql "REVOKE SELECT_PRIV ON information_schema.database_properties FROM ${user}"
    sql "GRANT SELECT_PRIV ON ${dbName} TO ${user}"
    connect(user, '123abc!@#', url) {
        qt_select_check_4 """
            select * from information_schema.database_properties
            where schema_name = "${dbName}"
            order by CATALOG_NAME, SCHEMA_NAME, PROPERTY_NAME, PROPERTY_VALUE
        """
    }
}
