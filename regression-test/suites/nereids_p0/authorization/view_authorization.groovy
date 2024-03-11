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

suite("view_authorization") {
    def db = context.config.getDbNameByFile(context.file)
    def user1 = "test_view_auth_user1"
    def baseTable = "test_view_auth_base_table"
    def view1 = "test_view_auth_view1"
    def view2 = "test_view_auth_view2"
    def view3 = "test_view_auth_view3"


    sql "drop table if exists ${baseTable}"
    sql "drop view if exists ${view1}"
    sql "drop view if exists ${view2}"
    sql "drop view if exists ${view3}"
    sql "drop user if exists ${user1}"

    sql """
        CREATE TABLE ${baseTable} (id INT, name TEXT)
            DISTRIBUTED BY HASH(`id`)
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql "insert into ${baseTable} values(1, 'hello'), (2, 'world'), (3, 'doris');"
    sql "create view ${view1} as select *, concat(name, '_', id) from ${db}.${baseTable} where id=1;"
    sql "create view ${view2} as select *, concat(name, '_', id) as xxx from ${db}.${baseTable} where id != 1;"
    sql "create view ${view3} as select xxx, 100 from ${db}.${view2} where id=3"

    sql "create user ${user1}"
    sql "grant SELECT_PRIV on ${db}.${view1} to '${user1}'@'%';"
    sql "grant SELECT_PRIV on ${db}.${view3} to '${user1}'@'%';"

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user1}""";
    }    

    sql 'sync'

    def defaultDbUrl = context.config.jdbcUrl.substring(0, context.config.jdbcUrl.lastIndexOf("/"))
    logger.info("connect to ${defaultDbUrl}".toString())
    connect(user = user1, password = null, url = defaultDbUrl) {
        sql "set enable_fallback_to_original_planner=false"

        // no privilege to base table
        test {
            sql "select * from ${db}.${baseTable}"
            exception "does not have privilege for"
        }

        // has privilege to view1
        test {
            sql "select * from ${db}.${view1}"
            result([[1, 'hello', 'hello_1']])
        }

        // no privilege to view2
        test {
            sql "select * from ${db}.${view2}"
            exception "does not have privilege for"
        }

        // nested view
        // has privilege to view3
        test {
            sql "select * from ${db}.${view3}"
            result([['doris_3', 100]])
        }
    }
}
