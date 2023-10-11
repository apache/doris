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

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.sql.PreparedStatement

suite("insert_group_commit_with_exception") {
    def table = "insert_group_commit_with_exception"

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
        while (true) {
            sleep(2000)
            def state = sql "show alter table column where tablename = '${table}' order by CreateTime desc "
            logger.info("alter table state: ${state}")
            if (state.size()> 0 && state[0][9] == "FINISHED") {
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
        CREATE TABLE `${table}` (
            `id` int(11) NOT NULL,
            `name` varchar(1100) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """

        sql """ set enable_insert_group_commit = true; """

        // insert into without column
        try {
            def result = sql """ insert into ${table} values(1, 'a', 10, 100)  """
            assertTrue(false)
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Column count doesn't match value count"))
        }

        try {
            def result = sql """ insert into ${table} values(2, 'b')  """
            assertTrue(false)
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Column count doesn't match value count"))
        }

        result = sql """ insert into ${table} values(3, 'c', 30)  """
        logger.info("insert result: " + result)

        // insert into with column
        result = sql """ insert into ${table}(id, name) values(4, 'd')  """
        logger.info("insert result: " + result)

        getRowCount(2)

        try {
            result = sql """ insert into ${table}(id, name) values(5, 'd', 50)  """
            assertTrue(false)
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Column count doesn't match value count"))
        }

        try {
            result = sql """ insert into ${table}(id, name) values(6)  """
            assertTrue(false)
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Column count doesn't match value count"))
        }

        try {
            result = sql """ insert into ${table}(id, names) values(7, 'd')  """
            assertTrue(false)
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unknown column 'names'"))
        }


        // prepare insert
        def db = context.config.defaultDb + "_insert_p0"
        String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, db)

        try (Connection connection = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword)) {
            Statement statement = connection.createStatement();
            statement.execute("use ${db}");
            statement.execute("set enable_insert_group_commit = true;");
            // without column
            try (PreparedStatement ps = connection.prepareStatement("insert into ${table} values(?, ?, ?, ?)")) {
                ps.setObject(1, 8);
                ps.setObject(2, "f");
                ps.setObject(3, 70);
                ps.setObject(4, "a");
                ps.addBatch();
                int[] result = ps.executeBatch();
                assertTrue(false)
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Column count doesn't match value count"))
            }

            try (PreparedStatement ps = connection.prepareStatement("insert into ${table} values(?, ?)")) {
                ps.setObject(1, 9);
                ps.setObject(2, "f");
                ps.addBatch();
                int[] result = ps.executeBatch();
                assertTrue(false)
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Column count doesn't match value count"))
            }

            try (PreparedStatement ps = connection.prepareStatement("insert into ${table} values(?, ?, ?)")) {
                ps.setObject(1, 10);
                ps.setObject(2, "f");
                ps.setObject(3, 90);
                ps.addBatch();
                int[] result = ps.executeBatch();
                logger.info("prepare insert result: " + result)
            }

            // with columns
            try (PreparedStatement ps = connection.prepareStatement("insert into ${table}(id, name) values(?, ?)")) {
                ps.setObject(1, 11);
                ps.setObject(2, "f");
                ps.addBatch();
                int[] result = ps.executeBatch();
                logger.info("prepare insert result: " + result)
            }

            try (PreparedStatement ps = connection.prepareStatement("insert into ${table}(id, name) values(?, ?, ?)")) {
                ps.setObject(1, 12);
                ps.setObject(2, "f");
                ps.setObject(3, "f");
                ps.addBatch();
                int[] result = ps.executeBatch();
                assertTrue(false)
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Column count doesn't match value count"))
            }

            try (PreparedStatement ps = connection.prepareStatement("insert into ${table}(id, name) values(?)")) {
                ps.setObject(1, 13);
                ps.addBatch();
                int[] result = ps.executeBatch();
                assertTrue(false)
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Column count doesn't match value count"))
            }

            try (PreparedStatement ps = connection.prepareStatement("insert into ${table}(id, names) values(?, ?)")) {
                ps.setObject(1, 12);
                ps.setObject(2, "f");
                ps.addBatch();
                int[] result = ps.executeBatch();
                assertTrue(false)
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Unknown column 'names'"))
            }

            getRowCount(4)

            // prepare insert with multi rows
            try (PreparedStatement ps = connection.prepareStatement("insert into ${table} values(?, ?, ?)")) {
                for (int i = 0; i < 5; i++) {
                    ps.setObject(1, 13 + i);
                    ps.setObject(2, "f");
                    ps.setObject(3, 90);
                    ps.addBatch();
                    int[] result = ps.executeBatch();
                    logger.info("prepare insert result: " + result)
                }
            }
            getRowCount(9)

            // prepare insert with multi rows
            try (PreparedStatement ps = connection.prepareStatement("insert into ${table} values(?, ?, ?),(?, ?, ?)")) {
                for (int i = 0; i < 2; i++) {
                    ps.setObject(1, 18 + i);
                    ps.setObject(2, "f");
                    ps.setObject(3, 90);
                    ps.setObject(4, 18 + i + 1);
                    ps.setObject(5, "f");
                    ps.setObject(6, 90);
                    ps.addBatch();
                    int[] result = ps.executeBatch();
                    logger.info("prepare insert result: " + result)
                }
            }
            getRowCount(13)

            // prepare insert without column names, and do schema change
            try (PreparedStatement ps = connection.prepareStatement("insert into ${table} values(?, ?, ?)")) {
                ps.setObject(1, 22)
                ps.setObject(2, "f")
                ps.setObject(3, 90)
                ps.addBatch()
                int[] result = ps.executeBatch()
                logger.info("prepare insert result: " + result)

                sql """ alter table ${table} ADD column age int after name; """
                assertTrue(getAlterTableState(), "add column should success")

                try {
                    ps.setObject(1, 23)
                    ps.setObject(2, "f")
                    ps.setObject(3, 90)
                    ps.addBatch()
                    result = ps.executeBatch()
                    assertTrue(false)
                } catch (Exception e) {
                    assertTrue(e.getMessage().contains("Column count doesn't match value count"))
                }
            }
            getRowCount(14)

            // prepare insert with column names, and do schema change
            try (PreparedStatement ps = connection.prepareStatement("insert into ${table}(id, name, score) values(?, ?, ?)")) {
                ps.setObject(1, 24)
                ps.setObject(2, "f")
                ps.setObject(3, 90)
                ps.addBatch()
                int[] result = ps.executeBatch()
                logger.info("prepare insert result: " + result)

                sql """ alter table ${table} DROP column age; """
                assertTrue(getAlterTableState(), "drop column should success")

                ps.setObject(1, 25)
                ps.setObject(2, "f")
                ps.setObject(3, 90)
                ps.addBatch()
                result = ps.executeBatch()
                logger.info("prepare insert result: " + result)
            }
            getRowCount(16)
        }
    } finally {
        // try_sql("DROP TABLE ${table}")
    }
}
