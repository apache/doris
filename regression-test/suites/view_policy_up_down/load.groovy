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

suite("test_view_policy_prepare", "p0,view_policy,restart_fe") {
    sql "DROP DATABASE IF EXISTS test_view_policy_replay_db"
    sql "CREATE DATABASE test_view_policy_replay_db"
    sql "USE test_view_policy_replay_db"
    for (user in ["test_view_policy_user", "test_view_policy_other"]) {
        sql "DROP USER IF EXISTS ${user}"
        sql "CREATE USER ${user} IDENTIFIED BY 'C123_567p'"
        sql "GRANT SELECT_PRIV ON test_view_policy_replay_db.* TO ${user}"
        if (isCloudMode()) {
            def clusterName = sql("SHOW CLUSTERS")[0][0]
            sql "GRANT USAGE_PRIV ON CLUSTER `${clusterName}` TO ${user}"
        }
    }
    sql "CREATE VIEW test_view_policy_min(user_name) AS SELECT 'alice' UNION ALL SELECT 'bob'"
    for (user in ["test_view_policy_user", "test_view_policy_other"]) {
        sql "GRANT SELECT_PRIV ON test_view_policy_replay_db.test_view_policy_min TO ${user}"
    }
    sql '''
CREATE VIEW test_view_policy_grants

(user_name,db_name,table_name,privilege,grant_sql)

 AS select

    SPLIT_BY_STRING(`internal`.`information_schema`.`table_privileges`.`GRANTEE`, '\\'')[2] AS `user_name`, `internal`.`information_schema`.`table_privileges`.`TABLE_SCHEMA` AS `db_name`, `internal`.`information_schema`.`table_privileges`.`TABLE_NAME` AS `table_name`, concat_ws(',', collect_set(replace(`internal`.`information_schema`.`table_privileges`.`PRIVILEGE_TYPE`,'INSERT','LOAD'))) AS `privilege`, lower(concat('grant ', concat_ws(',', collect_set(concat(replace(`internal`.`information_schema`.`table_privileges`.`PRIVILEGE_TYPE`,'INSERT','LOAD'), '_PRIV'))), ' on ', `internal`.`information_schema`.`table_privileges`.`TABLE_SCHEMA`,'.', `internal`.`information_schema`.`table_privileges`.`TABLE_NAME`,

              ' to ', SPLIT_BY_STRING(`internal`.`information_schema`.`table_privileges`.`GRANTEE`, '\\'')[2])) AS `grant_sql`

from `internal`.`information_schema`.`table_privileges`

where `internal`.`information_schema`.`table_privileges`.`PRIVILEGE_TYPE` not in ('SHOW VIEW')

group by `internal`.`information_schema`.`table_privileges`.`GRANTEE`, `internal`.`information_schema`.`table_privileges`.`TABLE_SCHEMA`,`internal`.`information_schema`.`table_privileges`.`TABLE_NAME`

union all

select

    SPLIT_BY_STRING(`internal`.`information_schema`.`schema_privileges`.`GRANTEE`, '\\'')[2] as `user_name`,

    `internal`.`information_schema`.`schema_privileges`.`TABLE_SCHEMA` as `db_name`,

    '*'   as `table_name`,

    concat_ws(',', collect_set(replace(`internal`.`information_schema`.`schema_privileges`.`PRIVILEGE_TYPE`,'INSERT','LOAD'))) `privilege`,

    lower(concat('grant ', concat_ws(',', collect_set(concat(replace(`internal`.`information_schema`.`schema_privileges`.`PRIVILEGE_TYPE`,'INSERT','LOAD'), '_PRIV'))), ' on ', `internal`.`information_schema`.`schema_privileges`.`TABLE_SCHEMA`,'.*',

              ' to ', SPLIT_BY_STRING(`internal`.`information_schema`.`schema_privileges`.`GRANTEE`, '\\'')[2])) as `grant_sql`

from `internal`.`information_schema`.`schema_privileges`

where `internal`.`information_schema`.`schema_privileges`.`PRIVILEGE_TYPE` not in ('SHOW VIEW')

group by `internal`.`information_schema`.`schema_privileges`.`GRANTEE`, `internal`.`information_schema`.`schema_privileges`.`TABLE_SCHEMA`;
    '''
    sql """CREATE ROW POLICY before_restart ON test_view_policy_replay_db.test_view_policy_grants
        AS RESTRICTIVE TO test_view_policy_user USING (user_name = 'test_view_policy_user')"""
    def url = context.config.jdbcUrl.replaceFirst('/[^/]*$', '/test_view_policy_replay_db?useLocalSessionState=true')
    connectToDoris("test_view_policy_user", "C123_567p", url) {
        order_qt_before_restart "SELECT user_name, table_name, privilege FROM test_view_policy_grants"
    }
    sql "DROP ROW POLICY before_restart ON test_view_policy_replay_db.test_view_policy_grants FOR test_view_policy_user"
}
