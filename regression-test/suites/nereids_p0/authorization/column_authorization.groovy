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

suite("column_authorization") {
    def db = context.config.getDbNameByFile(context.file)
    def user1 = "test_unique_table_auth_user1"
    def baseTable = "test_unique_table_auth_base_table"


    sql "drop table if exists ${baseTable}"

    sql """
        CREATE TABLE ${baseTable} (id INT, name TEXT)
            unique key(id)
            DISTRIBUTED BY HASH(`id`)
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql "insert into ${baseTable} values(1, 'hello'), (2, 'world'), (3, 'doris');"

    sql "drop user if exists ${user1}"
    sql "create user ${user1}"

    sql "grant SELECT_PRIV(id) on ${db}.${baseTable} to '${user1}'@'%';"
    sql "grant SELECT_PRIV(name) on ${db}.${baseTable} to '${user1}'@'%';"
    sql "revoke SELECT_PRIV(name) on ${db}.${baseTable} from '${user1}'@'%';"
    sql "revoke SELECT_PRIV(id) on ${db}.${baseTable} from '${user1}'@'%';"

    sql "grant SELECT_PRIV(id) on ${db}.${baseTable} to '${user1}'@'%';"

    sql 'sync'

    def defaultDbUrl = context.config.jdbcUrl.substring(0, context.config.jdbcUrl.lastIndexOf("/"))
    logger.info("connect to ${defaultDbUrl}".toString())
    connect(user1, null, defaultDbUrl) {
        sql "set enable_fallback_to_original_planner=false"

        // no privilege to name
        test {
            sql "select * from ${db}.${baseTable}"
            exception "Permission denied"
        }

        // has privilege to id, __DORIS_DELETE_SIGN__
        sql "select id, __DORIS_DELETE_SIGN__ from ${db}.${baseTable}"
    }
}
