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

import org.codehaus.groovy.runtime.IOGroovyMethods
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite ("test_modify_struct") {
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    def tableNamePrefix = "test_struct_add_sub_column"
    def tableName = tableNamePrefix
    int max_try_secs = 300

    try {

        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        // duplicate table
        tableName = tableNamePrefix + "_dup"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    `c0` LARGEINT NOT NULL,
                    `c_s` STRUCT<col:VARCHAR(10)>
                ) DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" )
            """

        sql """ insert into $tableName values
                (0, named_struct('col','commiter'));
            """
        sql """ insert into $tableName values
                (1, named_struct('col','amory'));
            """
        // this can be insert but with cut off the left string to 10
        test {
            sql """ insert into $tableName values
                (11, named_struct('col','amoryIsBetter'));
            """
            exception "Insert has filtered data in strict mode"
        }


        String[][] res = sql """ desc ${tableName} """
        logger.info(res[1][1])
        assertEquals(res[1][1].toLowerCase(),"struct<col:varchar(10)>")

        order_qt_sc_before " select * from ${tableName} order by c0; "

        // modify struct with varchar(20)
        sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(20)> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // check the struct column
        res = sql """ desc ${tableName} """
        logger.info(res[1][1])
        assertEquals(res[1][1].toLowerCase(),"struct<col:varchar(20)>")

        // insert some data to modified struct with varchar(20)
        sql """ insert into ${tableName} values
                (2, named_struct('col','amoryIsBetter'));
            """

        order_qt_sc_after " select * from ${tableName} order by c0; "


        // test struct add sub column
        sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:INT> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // check the struct column
        res = sql """ desc ${tableName} """
        logger.info(res[1][1])
        assertEquals(res[1][1].toLowerCase(),"struct<col:varchar(30),col1:int>")

        // insert some data to modified struct with varchar(30) and int
        sql """ insert into ${tableName} values
                (3, named_struct('col','amoryIsBetterCommiter', 'col1', 1));
            """

        order_qt_sc_after1 " select * from ${tableName} order by c0; "

        // test struct reduce sub-column behavior not support
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30)> """
            exception "Cannot change"
        }

        // test struct sub-column type change
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:INT, col1:INT> """
            exception "Cannot change"
        }

        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:VARCHAR(30)> """
            exception "Cannot change"
        }

        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:BIGINT> """
            exception "Cannot change"
        }

        // test struct sub-column name change
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col2:INT> """
            exception "Cannot change"
        }

        // test unique key table
        tableName = tableNamePrefix + "_unique"
        sql "DROP TABLE IF EXISTS ${tableName} FORCE;"
        sql """
            CREATE TABLE IF NOT EXISTS `${tableName}`
            (
                `siteid` INT DEFAULT '10',
                `citycode` SMALLINT,
                `c_s` STRUCT<col:VARCHAR(10)>,
            )
            UNIQUE KEY(`siteid`, `citycode`)
            DISTRIBUTED BY HASH(siteid) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql """ INSERT INTO ${tableName} VALUES
                (1, 1, named_struct('col','xxx')),
                (2, 2, named_struct('col','yyy')),
                (3, 3, named_struct('col','zzz'));
            """

        // check struct column
        res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"struct<col:varchar(10)>")

        // test unique key table modify struct column
        order_qt_sc_before " select * from ${tableName} order by siteid, citycode; "


        // test struct add sub column
        sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:INT> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // check the struct column
        res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"struct<col:varchar(30),col1:int>")

        // insert some data to modified struct with varchar(30) and int
        sql """ insert into ${tableName} values
                (3, 4, named_struct('col','amoryIsBetterCommiter', 'col1', 1));
            """
        sql """ insert into ${tableName} values
                (4, 4, named_struct('col','amoryIsBetterCommiterAgain', 'col1', 2));
            """
        

        order_qt_sc_after " select * from ${tableName} order by siteid, citycode; "

        // test struct reduce sub-column behavior not support
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30)> """
            exception "Cannot change"
        }

        // test struct sub-column type change
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:INT, col1:INT> """
            exception "Cannot change"
        }

        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:VARCHAR(30)> """
            exception "Cannot change"
        }

        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:BIGINT> """
            exception "Cannot change"
        }

        // test struct sub-column name change
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col2:INT> """
            exception "Cannot change"
        }

        // test MOW table
        tableName = tableNamePrefix + "_mow"
        sql "DROP TABLE IF EXISTS ${tableName} FORCE;"
        sql """
            CREATE TABLE IF NOT EXISTS `${tableName}`
            (
                `siteid` INT DEFAULT '10',
                `citycode` SMALLINT,
                `c_s` STRUCT<col:VARCHAR(10)>,
            )
            UNIQUE KEY(`siteid`, `citycode`)
            DISTRIBUTED BY HASH(siteid) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """

        sql """ INSERT INTO ${tableName} VALUES
                (1, 1, named_struct('col','xxx')),
                (2, 2, named_struct('col','yyy')),
                (3, 3, named_struct('col','zzz'));
            """

        // check struct column
        res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"struct<col:varchar(10)>")

        // test MOW table modify struct column
        order_qt_sc_before " select * from ${tableName} order by siteid, citycode; "


        // test struct add sub column
        sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:INT> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });

        // check the struct column
        res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"struct<col:varchar(30),col1:int>")

        // insert some data to modified struct with varchar(30) and int
        sql """ insert into ${tableName} values
                (3, 4, named_struct('col','amoryIsBetterCommiter', 'col1', 1));
            """
        sql """ insert into ${tableName} values
                (4, 4, named_struct('col','amoryIsBetterCommiterAgain', 'col1', 2));
            """

        order_qt_sc_after " select * from ${tableName} order by siteid, citycode; "

        // test struct reduce sub-column behavior not support
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30)> """
            exception "Cannot change"
        }

        // test struct sub-column type change
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:INT, col1:INT> """
            exception "Cannot change"
        }

        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:VARCHAR(30)> """
            exception "Cannot change"
        }

        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:BIGINT> """
            exception "Cannot change"
        }

        // test struct sub-column name change
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col2:INT> """
            exception "Cannot change"
        }

        // test agg table
        tableName = tableNamePrefix + "_agg"
        sql "DROP TABLE IF EXISTS ${tableName} FORCE;"
        sql """
            CREATE TABLE IF NOT EXISTS `${tableName}`
            (
                `siteid` INT DEFAULT '10',
                `citycode` SMALLINT,
                `c_s` STRUCT<col:VARCHAR(10)> REPLACE,
            )
            AGGREGATE KEY(`siteid`, `citycode`)
            DISTRIBUTED BY HASH(siteid) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        sql """ INSERT INTO ${tableName} VALUES
                (1, 1, named_struct('col','xxx')),
                (2, 2, named_struct('col','yyy')),
                (3, 3, named_struct('col','zzz'));
            """

        // check struct column
        res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"struct<col:varchar(10)>")

        // test MOW table modify struct column
        order_qt_sc_before " select * from ${tableName} order by siteid, citycode; "

        // test struct add sub column
        sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:INT> REPLACE """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });

        // check the struct column
        res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"struct<col:varchar(30),col1:int>")

        // insert some data to modified struct with varchar(30) and int
        sql """ insert into ${tableName} values
                (3, 4, named_struct('col','amoryIsBetterCommiter', 'col1', 1));
            """
        sql """ insert into ${tableName} values
                (4, 4, named_struct('col','amoryIsBetterCommiterAgain', 'col1', 2));
            """

        order_qt_sc_after " select * from ${tableName} order by siteid, citycode; "

        // test agg table for not change agg type
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:INT> REPLACE_IF_NOT_NULL """
            exception "Can not change aggregation type"
        }

        // test struct reduce sub-column behavior not support
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30)> REPLACE"""
            exception "Cannot change"
        }

        // test struct sub-column type change
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:INT, col1:INT> REPLACE"""
            exception "Cannot change"
        }

        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:VARCHAR(30)> REPLACE"""
            exception "Cannot change"
        }

        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col1:BIGINT> REPLACE"""
            exception "Cannot change"
        }

        // test struct sub-column name change
        test {
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(30), col2:INT> REPLACE"""
            exception "Cannot change"
        }

    } finally {
         try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

}
