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
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("insert_group_commit_into") {
    def dbName = "regression_test_insert_p0"
    def tableName = "insert_group_commit_into_duplicate"
    def table = dbName + "." + tableName

    def getRowCount = { expectedRowCount ->
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until(
            {
                def result = sql "select count(*) from ${table}"
                logger.info("table: ${table}, rowCount: ${result}")
                return result[0][0] == expectedRowCount
            }
        )
    }

    def getAlterTableState = {
        sql "use ${dbName};"
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
        return true
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
        return serverInfo
    }

    def group_commit_insert_with_retry = { sql, expected_row_count ->
        def retry = 0
        while (true){
            try {
                return group_commit_insert(sql, expected_row_count)
            } catch (Exception e) {
                logger.warn("group_commit_insert failed, retry: " + retry + ", error: " + e.getMessage())
                retry++
                if (e.getMessage().contains("is blocked on schema change") && retry < 20) {
                    sleep(1500)
                    continue
                } else {
                    throw e
                }
            }
        }
    }

    def none_group_commit_insert = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'VISIBLE'"))
        assertTrue(!serverInfo.contains("'label':'group_commit_"))
    }

    def txn_insert = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'PREPARE'"))
        assertTrue(!serverInfo.contains("'label':'group_commit_"))
    }

    for (item in ["legacy", "nereids"]) {
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

            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl + "&useLocalSessionState=true") {
                sql """ set group_commit = async_mode; """
                if (item == "nereids") {
                    sql """ set enable_nereids_dml = true; """
                    sql """ set enable_nereids_planner=true; """
                    sql """ set enable_fallback_to_original_planner=false; """
                } else {
                    sql """ set enable_nereids_dml = false; """
                }

                // 1. insert into
                group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
                group_commit_insert """ insert into ${table}(id) values(4);  """, 1
                group_commit_insert """ insert into ${table} values (1, 'a', 10),(5, 'q', 50); """, 2
                group_commit_insert """ insert into ${table}(id, name) values(2, 'b'); """, 1
                if (item == "nereids") {
                    none_group_commit_insert """ insert into ${table}(id) select 6; """, 1
                } else {
                    group_commit_insert """ insert into ${table}(id) select 6; """, 1
                }

                getRowCount(6)
                order_qt_select1 """ select * from ${table} order by id, name, score asc; """

                // 2. insert into and delete
                sql """ delete from ${table} where id = 4; """
                group_commit_insert """ insert into ${table}(name, id) values('c', 3); """, 1
                /*sql """ insert into ${table}(id, name) values(4, 'd1');  """
                sql """ insert into ${table}(id, name) values(4, 'd1');  """
                sql """ delete from ${table} where id = 4; """*/
                group_commit_insert """ insert into ${table}(id, name) values(4, 'e1'); """, 1
                group_commit_insert """ insert into ${table} values (1, 'a', 10),(5, 'q', 50); """, 2
                group_commit_insert """ insert into ${table}(id, name) values(2, 'b'); """, 1
                group_commit_insert """ insert into ${table}(id) values(6); """, 1

                getRowCount(11)
                order_qt_select2 """ select * from ${table} order by id, name, score asc; """

                // 3. insert into and light schema change: add column
                group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
                group_commit_insert """ insert into ${table}(id) values(4);  """, 1
                group_commit_insert """ insert into ${table} values (1, 'a', 10),(5, 'q', 50);  """, 2
                sql """ alter table ${table} ADD column age int after name; """
                group_commit_insert_with_retry """ insert into ${table}(id, name) values(2, 'b');  """, 1
                group_commit_insert_with_retry """ insert into ${table}(id) values(6); """, 1

                assertTrue(getAlterTableState(), "add column should success")
                getRowCount(17)
                order_qt_select3 """ select * from ${table} order by id, name,score asc; """

                // 4. insert into and truncate table
                /*sql """ insert into ${table}(name, id) values('c', 3);  """
                sql """ insert into ${table}(id) values(4);  """
                sql """ insert into ${table} values (1, 'a', 5, 10),(5, 'q', 6, 50);  """*/
                sql """ truncate table ${table}; """
                group_commit_insert """ insert into ${table}(id, name) values(2, 'b');  """, 1
                group_commit_insert """ insert into ${table}(id) values(6); """, 1

                getRowCount(2)
                order_qt_select4 """ select * from ${table} order by id, name, score asc; """

                // 5. insert into and schema change: modify column order
                group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
                group_commit_insert """ insert into ${table}(id) values(4);  """, 1
                group_commit_insert """ insert into ${table}(id, name, age, score) values (1, 'a', 5, 10),(5, 'q', 6, 50);  """, 2
                sql """ alter table ${table} order by (id, name, score, age); """
                group_commit_insert_with_retry """ insert into ${table}(id, name) values(2, 'b');  """, 1
                group_commit_insert_with_retry """ insert into ${table}(id) values(6); """, 1

                assertTrue(getAlterTableState(), "modify column order should success")
                getRowCount(8)
                order_qt_select5 """ select id, name, score, age from ${table} order by id, name, score asc; """

                // 6. insert into and light schema change: drop column
                group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
                group_commit_insert """ insert into ${table}(id) values(4);  """, 1
                group_commit_insert """ insert into ${table}(id, name, age, score) values (1, 'a', 5, 10),(5, 'q', 6, 50);  """, 2
                sql """ alter table ${table} DROP column age; """
                group_commit_insert_with_retry """ insert into ${table}(id, name) values(2, 'b');  """, 1
                group_commit_insert_with_retry """ insert into ${table}(id) values(6); """, 1

                assertTrue(getAlterTableState(), "drop column should success")
                getRowCount(14)
                order_qt_select6 """ select * from ${table} order by id, name, score asc; """

                // 7. insert into and add rollup
                group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
                group_commit_insert """ insert into ${table}(id) values(4);  """, 1
                sql "set enable_insert_strict=false"
                group_commit_insert """ insert into ${table} values (1, 'a', 10),(5, 'q', 50),(101, 'a', 100);  """, 2
                sql "set enable_insert_strict=true"
                try {
                    sql """ insert into ${table} values (102, 'a', 100);  """
                    assertTrue(false, "insert should fail")
                } catch (Exception e) {
                    logger.info("error: " + e.getMessage())
                    assertTrue(e.getMessage().contains("url:"))
                }
                sql """ alter table ${table} ADD ROLLUP r1(name, score); """
                group_commit_insert_with_retry """ insert into ${table}(id, name) values(2, 'b');  """, 1
                group_commit_insert_with_retry """ insert into ${table}(id) values(6); """, 1

                getRowCount(20)
                order_qt_select7 """ select name, score from ${table} order by name asc; """
                assertTrue(getAlterTableState(), "add rollup should success")

                if (item == "nereids") {
                    group_commit_insert """ insert into ${table}(id, name, score) values(10 + 1, 'h', 100);  """, 1
                    none_group_commit_insert """ insert into ${table}(id, name, score) select 10 + 2, 'h', 100;  """, 1
                    group_commit_insert """ insert into ${table} with label test_gc_""" + System.currentTimeMillis() + """ (id, name, score) values(13, 'h', 100);  """, 1
                    getRowCount(23)
                } else {
                    none_group_commit_insert """ insert into ${table}(id, name, score) values(10 + 1, 'h', 100);  """, 1
                    none_group_commit_insert """ insert into ${table}(id, name, score) select 10 + 2, 'h', 100;  """, 1
                    none_group_commit_insert """ insert into ${table} with label test_gc_""" + System.currentTimeMillis() + """ (id, name, score) values(13, 'h', 100);  """, 1
                }

                def rowCount = sql "select count(*) from ${table}"
                logger.info("row count: " + rowCount)
                assertEquals(23, rowCount[0][0])

                // txn insert
                def stmt = prepareStatement """ begin  """
                stmt.executeUpdate()
                txn_insert """ insert into ${table}(id, name, score) values(20, 'i', 101);  """, 1
                txn_insert """ insert into ${table}(id, name, score) values(21, 'j', 102);  """, 1
                stmt = prepareStatement """ commit  """
                stmt.executeUpdate()

                rowCount = sql "select count(*) from ${table}"
                logger.info("row count: " + rowCount)
                assertEquals(rowCount[0][0], 25)
            }
        } finally {
            // try_sql("DROP TABLE ${table}")
        }

        // test connect to observer fe
        try {
            def fes = sql_return_maparray "show frontends"
            logger.info("frontends: ${fes}")
            if (fes.size() > 1) {
                def observer_fe = null
                for (def fe : fes) {
                    if (fe.IsMaster == "false") {
                        observer_fe = fe
                        break
                    }
                }
                if (observer_fe != null) {
                    def url = "jdbc:mysql://${observer_fe.Host}:${observer_fe.QueryPort}/"
                    logger.info("observer url: " + url)
                    connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = url) {
                        sql """ set group_commit = async_mode; """

                        // 1. insert into
                        def server_info = group_commit_insert """ insert into ${table}(name, id) values('c', 3);  """, 1
                        /*assertTrue(server_info.contains('query_id'))
                        // get query_id, such as 43f87963586a482a-b0496bcf9e2b5555
                        def query_id_index = server_info.indexOf("'query_id':'") + "'query_id':'".length()
                        def query_id = server_info.substring(query_id_index, query_id_index + 33)
                        logger.info("query_id: " + query_id)
                        // 2. check profile
                        StringBuilder sb = new StringBuilder();
                        sb.append("curl -X GET -u ${context.config.jdbcUser}:${context.config.jdbcPassword} http://${observer_fe.Host}:${observer_fe.HttpPort}")
                        sb.append("/api/profile?query_id=").append(query_id)
                        String command = sb.toString()
                        logger.info(command)
                        def process = command.execute()
                        def code = process.waitFor()
                        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                        def out = process.getText()
                        logger.info("Get profile: code=" + code + ", out=" + out + ", err=" + err)
                        assertEquals(code, 0)
                        def json = parseJson(out)
                        assertEquals("success", json.msg.toLowerCase())*/
                    }
                }
            } else {
                logger.info("only one fe, skip test connect to observer fe")
            }
        } finally {
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
                sql """ set group_commit = async_mode; """
                if (item == "nereids") {
                    sql """ set enable_nereids_dml = true; """
                    sql """ set enable_nereids_planner=true; """
                    sql """ set enable_fallback_to_original_planner=false; """
                } else {
                    sql """ set enable_nereids_dml = false; """
                }

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

        // table with MaterializedView
        tableName = "insert_group_commit_into_mv"
        table = dbName + "." + tableName
        def table_tmp = dbName + ".test_table_tmp"
        try {
            // create table
            sql """ drop table if exists ${table}; """
            sql """CREATE table ${table} (
            `ordernum` varchar(65533) NOT NULL ,
            `dnt` datetime NOT NULL ,
            `data` json NULL 
            ) ENGINE=OLAP
            DUPLICATE KEY(`ordernum`, `dnt`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`ordernum`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""
            sql """drop table if exists ${table_tmp};"""
            sql """CREATE TABLE ${table_tmp} (
            `dnt` varchar(200) NULL,
            `ordernum` varchar(200) NULL,
            `type` varchar(20) NULL,
            `powers` double SUM NULL,
            `p0` double REPLACE NULL,
            `heatj` double SUM NULL,
            `j0` double REPLACE NULL,
            `heatg` double SUM NULL,
            `g0` double REPLACE NULL,
            `solar` double SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`dnt`, `ordernum`, `type`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`ordernum`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            ); """
            sql """DROP MATERIALIZED VIEW IF EXISTS ods_zn_dnt_max1 ON ${table};"""
            sql """create materialized view ods_zn_dnt_max1 as
            select ordernum,max(dnt) as dnt from ${table}
            group by ordernum
            ORDER BY ordernum;"""
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
                sql """ set group_commit = async_mode; """
                if (item == "nereids") {
                    sql """ set enable_nereids_dml = true; """
                    sql """ set enable_nereids_planner=true; """
                    sql """ set enable_fallback_to_original_planner=false; """
                } else {
                    sql """ set enable_nereids_dml = false; """
                }

                // 1. insert into
                int count = 0;
                while (count < 30) {
                    try {
                        group_commit_insert """ 
                insert into ${table} values('cib2205045_1_1s','2023/6/10 3:55:33','{"DB1":168939,"DNT":"2023-06-10 03:55:33"}');""", 1
                        break
                    } catch (Exception e) {
                        logger.info("got exception:" + e)
                        if (e.getMessage().contains("is blocked on schema change")) {
                            Thread.sleep(1000)
                        }
                        count++
                    }
                }
                group_commit_insert """insert into ${table} values('cib2205045_1_1s','2023/6/10 3:56:33','{"DB1":168939,"DNT":"2023-06-10 03:56:33"}');""", 1
                group_commit_insert """insert into ${table} values('cib2205045_1_1s','2023/6/10 3:57:33','{"DB1":168939,"DNT":"2023-06-10 03:57:33"}');""", 1
                group_commit_insert """insert into ${table} values('cib2205045_1_1s','2023/6/10 3:58:33','{"DB1":168939,"DNT":"2023-06-10 03:58:33"}');""", 1

                getRowCount(4)

                qt_order """select
                '2023-06-10',
                tmp.ordernum,
                cast(nvl(if(tmp.p0-tmp1.p0>0,tmp.p0-tmp1.p0,tmp.p0-tmp.p1),0) as decimal(10,4)),
                nvl(tmp.p0,0),
                cast(nvl(if(tmp.j0-tmp1.j0>0,tmp.j0-tmp1.j0,tmp.j0-tmp.j1)*277.78,0) as decimal(10,4)),
                nvl(tmp.j0,0),
                cast(nvl(if(tmp.g0-tmp1.g0>0,tmp.g0-tmp1.g0,tmp.g0-tmp.g1)*277.78,0) as decimal(10,4)),
                nvl(tmp.g0,0),
                cast(nvl(tmp.solar,0) as decimal(20,4)),
                'day'
                from 
                (
                select
                    ordernum,
                    max(ljrl1) g0,min(ljrl1) g1,
                    max(ljrl2) j0,min(ljrl2) j1,
                    max(db1) p0,min(db1) p1,
                    max(fzl)*1600*0.278 solar
                from(
                    select ordernum,dnt,
                            cast(if(json_extract(data,'\$.LJRL1')=0 or json_extract(data,'\$.LJRL1') like '%E%',null,json_extract(data,'\$.LJRL1')) as double) ljrl1,
                            cast(if(json_extract(data,'\$.LJRL2')=0 or json_extract(data,'\$.LJRL2') like '%E%',null,json_extract(data,'\$.LJRL2')) as double) ljrl2,
                            first_value(cast(if(json_extract(data,'\$.FZL')=0 or json_extract(data,'\$.FZL') like '%E%',null,
                            json_extract(data,'\$.FZL')) as double)) over (partition by ordernum order by dnt desc) fzl,
                            cast(if(json_extract(data,'\$.DB1')=0 or json_extract(data,'\$.DB1') like '%E%',null,json_extract(data,'\$.DB1')) as double) db1
                    from ${table}
                        )a1
                group by ordernum
                )tmp left join (
                select
                    ordernum,MAX(p0) p0,MAX(j0) j0,MAX(g0) g0
                from ${table_tmp}
                    group by ordernum
                )tmp1
                on tmp.ordernum=tmp1.ordernum;"""
                qt_order2 """
                SELECT  
                row_number() over(partition by add_date order by pc_num desc)
                ,row_number() over(partition by add_date order by vc_num desc)
                ,row_number() over(partition by add_date order by vt_num desc)
                FROM (
                SELECT  
                cast(dnt as datev2) add_date
                ,row_number() over(order by dnt) pc_num
                ,row_number() over(order by dnt) vc_num
                ,row_number() over(order by dnt) vt_num
                FROM ${table}
                ) t;"""
            }
        } finally {
        }

        // column name contains keyword
        tableName = "insert_group_commit_into_with_keyword"
        table = dbName + "." + tableName
        try {
            // create table
            sql """ drop table if exists ${table}; """
            sql """
                CREATE TABLE IF NOT EXISTS ${table}
                (
                    k1 INT,
                    `or` varchar(50)
                )
                DUPLICATE KEY(`k1`)
                DISTRIBUTED BY HASH(`k1`) 
                BUCKETS 1 PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                ); 
            """

            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
                sql """ set group_commit = async_mode; """
                if (item == "nereids") {
                    sql """ set enable_nereids_dml = true; """
                    sql """ set enable_nereids_planner = true; """
                    sql """ set enable_fallback_to_original_planner = false; """
                } else {
                    sql """ set enable_nereids_dml = false; """
                }
                group_commit_insert """ insert into ${table} values(1, 'test'); """, 1
                group_commit_insert """ insert into ${table}(k1,`or`) values (2,"or"); """, 1
                getRowCount(2)
                order_qt_select8 """ select * from ${table}; """
            }
        } finally {
        }
    }
}
