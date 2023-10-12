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

import com.mysql.cj.jdbc.StatementImpl

suite("insert_group_commit_into_duplicate") {
    def dbName = "regression_test_insert_p0"
    def tableName = "insert_group_commit_into_duplicate"
    def table = dbName + "." + tableName

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${table}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    def getAlterTableState = {
        def retry = 0
        sql "use ${dbName};"
        while (true) {
            sleep(2000)
            def state = sql " show alter table column where tablename = '${tableName}' order by CreateTime desc "
            logger.info("alter table state: ${state}")
            if (state.size() > 0 && state[0][9] == "FINISHED") {
                return true
            }
            retry++
            if (retry >= 10) {
                return false
            }
        }
        return false
    }

    try {
        // create table
        sql """ drop table if exists ${table}; """

        sql """
        CREATE TABLE ${table} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        PARTITION BY RANGE(id)
        (
            FROM (1) TO (100) INTERVAL 10
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """

        def group_commit_insert = { sql, expected_row_count ->
            def stmt = prepareStatement """ ${sql}  """
            def result = stmt.executeUpdate()
            logger.info("insert result: " + result)
            def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
            logger.info("result server info: " + serverInfo)
            if (result != expected_row_count) {
                logger.warn("insert result: " + result + ", expected_row_count: " + expected_row_count + ", sql: " + sql)
            }
            // assertEquals(result, expected_row_count)
            assertTrue(serverInfo.contains("'status':'PREPARE'"))
            assertTrue(serverInfo.contains("'label':'group_commit_"))
        }

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql """ set enable_insert_group_commit = true; """
            sql """ set enable_nereids_dml = false; """

            // 1. insert into
            group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
            group_commit_insert """ insert into ${table}(id) values(4);  """, 1
            group_commit_insert """ insert into ${table} values (1, 'a', 10),(5, 'q', 50); """, 2
            group_commit_insert """ insert into ${table}(id, name) values(2, 'b'); """, 1
            group_commit_insert """ insert into ${table}(id) select 6; """, 1

            getRowCount(6)
            qt_sql """ select * from ${table} order by id, name, score asc; """

            // 2. insert into and delete
            sql """ delete from ${table} where id = 4; """
            group_commit_insert """ insert into ${table}(name, id) values('c', 3); """, 1
            /*sql """ insert into ${table}(id, name) values(4, 'd1');  """
            sql """ insert into ${table}(id, name) values(4, 'd1');  """
            sql """ delete from ${table} where id = 4; """*/
            group_commit_insert """ insert into ${table}(id, name) values(4, 'e1'); """, 1
            group_commit_insert """ insert into ${table} values (1, 'a', 10),(5, 'q', 50); """, 2
            group_commit_insert """ insert into ${table}(id, name) values(2, 'b'); """, 1
            group_commit_insert """ insert into ${table}(id) select 6; """, 1

            getRowCount(11)
            qt_sql """ select * from ${table} order by id, name, score asc; """

            // 3. insert into and light schema change: add column
            group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
            group_commit_insert """ insert into ${table}(id) values(4);  """, 1
            group_commit_insert """ insert into ${table} values (1, 'a', 10),(5, 'q', 50);  """, 2
            sql """ alter table ${table} ADD column age int after name; """
            group_commit_insert """ insert into ${table}(id, name) values(2, 'b');  """, 1
            group_commit_insert """ insert into ${table}(id) select 6; """, 1

            assertTrue(getAlterTableState(), "add column should success")
            getRowCount(17)
            qt_sql """ select * from ${table} order by id, name,score asc; """

            // 4. insert into and truncate table
            /*sql """ insert into ${table}(name, id) values('c', 3);  """
            sql """ insert into ${table}(id) values(4);  """
            sql """ insert into ${table} values (1, 'a', 5, 10),(5, 'q', 6, 50);  """*/
            sql """ truncate table ${table}; """
            group_commit_insert """ insert into ${table}(id, name) values(2, 'b');  """, 1
            group_commit_insert """ insert into ${table}(id) select 6; """, 1

            getRowCount(2)
            qt_sql """ select * from ${table} order by id, name, score asc; """

            // 5. insert into and schema change: modify column order
            group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
            group_commit_insert """ insert into ${table}(id) values(4);  """, 1
            group_commit_insert """ insert into ${table} values (1, 'a', 5, 10),(5, 'q', 6, 50);  """, 2
            // sql """ alter table ${table} order by (id, name, score, age); """
            group_commit_insert """ insert into ${table}(id, name) values(2, 'b');  """, 1
            group_commit_insert """ insert into ${table}(id) select 6; """, 1

            // assertTrue(getAlterTableState(), "modify column order should success")
            getRowCount(8)
            qt_sql """ select id, name, score, age from ${table} order by id, name, score asc; """

            // 6. insert into and light schema change: drop column
            group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
            group_commit_insert """ insert into ${table}(id) values(4);  """, 1
            group_commit_insert """ insert into ${table} values (1, 'a', 5, 10),(5, 'q', 6, 50);  """, 2
            sql """ alter table ${table} DROP column age; """
            group_commit_insert """ insert into ${table}(id, name) values(2, 'b');  """, 1
            group_commit_insert """ insert into ${table}(id) select 6; """, 1

            assertTrue(getAlterTableState(), "drop column should success")
            getRowCount(14)
            qt_sql """ select * from ${table} order by id, name, score asc; """

            // 7. insert into and add rollup
            group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
            group_commit_insert """ insert into ${table}(id) values(4);  """, 1
            group_commit_insert """ insert into ${table} values (1, 'a', 10),(5, 'q', 50),(101, 'a', 100);  """, 2
            // sql """ alter table ${table} ADD ROLLUP r1(name, score); """
            group_commit_insert """ insert into ${table}(id, name) values(2, 'b');  """, 1
            group_commit_insert """ insert into ${table}(id) select 6; """, 1

            getRowCount(20)
            qt_sql """ select name, score from ${table} order by name asc; """
        }
    } finally {
        // try_sql("DROP TABLE ${table}")
    }
}
