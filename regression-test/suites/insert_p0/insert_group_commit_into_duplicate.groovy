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

    def none_group_commit_insert = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        if (result != expected_row_count) {
            logger.warn("insert result: " + result + ", expected_row_count: " + expected_row_count + ", sql: " + sql)
        }
        // assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'VISIBLE'"))
        assertTrue(!serverInfo.contains("'label':'group_commit_"))
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

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql """ set enable_insert_group_commit = true; """
            // TODO
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

            none_group_commit_insert """ insert into ${table}(id, name, score) values(10 + 1, 'h', 100);  """, 1
            none_group_commit_insert """ insert into ${table}(id, name, score) select 10 + 2, 'h', 100;  """, 1
            none_group_commit_insert """ insert into ${table} with label test_gc_""" + System.currentTimeMillis() + """ (id, name, score) values(13, 'h', 100);  """, 1
            def rowCount = sql "select count(*) from ${table}"
            logger.info("row count: " + rowCount)
            assertEquals(rowCount[0][0], 23)
        }
    } finally {
        // try_sql("DROP TABLE ${table}")
    }

    // table with array type
    tableName = "insert_group_commit_into_duplicate_array"
    table = dbName + "." + tableName
    try {
        // create table
        sql """ drop table if exists ${table}; """

        sql """
        CREATE table ${table} (
            teamID varchar(255),
            service_id varchar(255),
            start_time BigInt,
            time_bucket BigInt ,
            segment_id String ,
            trace_id String ,
            data_binary String ,
            end_time BigInt ,
            endpoint_id String ,
            endpoint_name String ,
            is_error Boolean ,
            latency Int ,
            service_instance_id String ,
            statement String ,
            tags Array<String>
        ) UNIQUE key (`teamID`,`service_id`, `start_time`)
        DISTRIBUTED BY hash(`start_time`)
        BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql """ set enable_insert_group_commit = true; """
            // TODO
            sql """ set enable_nereids_dml = false; """

            // 1. insert into
            group_commit_insert """ 
            INSERT INTO ${table} (`data_binary`, `end_time`, `endpoint_id`, `endpoint_name`, `is_error`, `latency`, `segment_id`, `service_id`, `service_instance_id`, `start_time`, `statement`, `tags`, `teamID`, `time_bucket`, `trace_id`) 
            VALUES 
            ('CgEwEiQzMjI5YjdjZC1mM2EyLTQzNTktYWEyNC05NDYzODhjOWNjNTQaggQY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAEY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAIY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAMY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAQY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAUY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAYY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAcY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAgY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAkY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAoY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAsY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECAwY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECA0Y/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECA4Y/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECA8Y/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECBAY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECBEY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECBIY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5GoQECBMY/6n597ExIP+p+fexMWIWCgh0YWdLZXlfMBIKdGFnVmFsdWVfMGIWCgh0YWdLZXlfMRIKdGFnVmFsdWVfMWIWCgh0YWdLZXlfMhIKdGFnVmFsdWVfMmIWCgh0YWdLZXlfMxIKdGFnVmFsdWVfM2IWCgh0YWdLZXlfNBIKdGFnVmFsdWVfNGIWCgh0YWdLZXlfNRIKdGFnVmFsdWVfNWIWCgh0YWdLZXlfNhIKdGFnVmFsdWVfNmIWCgh0YWdLZXlfNxIKdGFnVmFsdWVfN2IWCgh0YWdLZXlfOBIKdGFnVmFsdWVfOGIWCgh0YWdLZXlfORIKdGFnVmFsdWVfOWIYCgl0YWdLZXlfMTASC3RhZ1ZhbHVlXzEwYhgKCXRhZ0tleV8xMRILdGFnVmFsdWVfMTFiGAoJdGFnS2V5XzEyEgt0YWdWYWx1ZV8xMmIYCgl0YWdLZXlfMTMSC3RhZ1ZhbHVlXzEzYhgKCXRhZ0tleV8xNBILdGFnVmFsdWVfMTRiGAoJdGFnS2V5XzE1Egt0YWdWYWx1ZV8xNWIYCgl0YWdLZXlfMTYSC3RhZ1ZhbHVlXzE2YhgKCXRhZ0tleV8xNxILdGFnVmFsdWVfMTdiGAoJdGFnS2V5XzE4Egt0YWdWYWx1ZV8xOGIYCgl0YWdLZXlfMTkSC3RhZ1ZhbHVlXzE5IixzZXJ2aWNlXzQ2ZGEwZGFiLWUyN2QtNDgyMC1hZWEyLTliZmMxNTc0MTYxNSo0c2VydmljZV9pbnN0YW5jZWFjODlhNGI3LTgxZjctNDNlOC04NWVkLWQyYjU3OGQ5ODA1MA==', 
            1697032066304, '36b2d9ff-4c25-49f3-a726-eea812564411', '355f96cd-b1b1-4688-a5f6-a8e3f3a55c9a', false, 3, '3229b7cd-f3a2-4359-aa24-946388c9cc54', 'service_46da0dab-e27d-4820-aea2-9bfc15741615', 'service_instanceac89a4b7-81f7-43e8-85ed-d2b578d98050', 1697032066304, 'statement: b9903670-3821-4f4c-a587-bbcf02c04b77', ['[tagKey_5=tagValue_5, tagKey_3=tagValue_3, tagKey_1=tagValue_1, tagKey_16=tagValue_16, tagKey_8=tagValue_8, tagKey_15=tagValue_15, tagKey_6=tagValue_6, tagKey_11=tagValue_11, tagKey_10=tagValue_10, tagKey_4=tagValue_4, tagKey_13=tagValue_13, tagKey_14=tagValue_14, tagKey_2=tagValue_2, tagKey_17=tagValue_17, tagKey_19=tagValue_19, tagKey_0=tagValue_0, tagKey_18=tagValue_18, tagKey_9=tagValue_9, tagKey_7=tagValue_7, tagKey_12=tagValue_12]'], '0', 0, '0');  
            """, 1

            getRowCount(1)
            qt_sql """ select * from ${table}; """
        }
    } finally {
        // try_sql("DROP TABLE ${table}")
    }
}
