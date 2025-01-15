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

suite ("test_varchar_sc_in_complex") {
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    def tableName = "test_varchar_sc_in_complex"

    try {

        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    `c0` LARGEINT NOT NULL,
                    `c_a` ARRAY<VARCHAR(10)>,
                    `c_m` MAP<VARCHAR(10),VARCHAR(10)>,
                    `c_s` STRUCT<col:VARCHAR(10)>
                ) DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" )
            """

        sql """ insert into ${tableName} values
                (0,['2025-01-01'], {'amory':'better'}, named_struct('col','commiter'));
            """
        sql """ insert into ${tableName} values
                (1,['2025-01-02'], {'doris':'better'}, named_struct('col','amory'));
            """
        // this can be insert but with cut off the left string to 10
        test {
            sql """ insert into ${tableName} values
                (11, ['2025-01-03-22-33'], {'doris111111111':'better2222222222'}, named_struct('col','amoryIsBetter'));
            """
            exception "Insert has filtered data in strict mode"
        }

        test {
            sql """ alter table ${tableName} modify column c_a array<varchar(3)>
                """
            exception "Cannot shorten string length"
        }

        test {
            sql """ alter table ${tableName} modify column c_m map<varchar(3),varchar(3)>
                """
            exception "Cannot shorten string length"
        }

        test {
            sql """ alter table ${tableName} modify column c_s struct<col:varchar(3)>
                """
            exception "Cannot shorten string length"
        }

        // add case alter modify array/map/struct to other type
        // test array to struct
        test {
            sql """ alter table ${tableName} modify column c_a struct<col:varchar(3)>
                """
            exception "Can not change ARRAY to STRUCT"
        }
        // test array to map
        test {
            sql """ alter table ${tableName} modify column c_a map<varchar(3),varchar(3)>
                """
            exception "Can not change ARRAY to MAP"
        }
        // test map to array
        test {
            sql """ alter table ${tableName} modify column c_m array<varchar(3)>
                """
            exception "Can not change MAP to ARRAY"
        }
        // test map to struct
        test {
            sql """ alter table ${tableName} modify column c_m struct<col:varchar(3)>
                """
            exception "Can not change MAP to STRUCT"
        }
        // test struct to array
        test {
            sql """ alter table ${tableName} modify column c_s array<varchar(3)>
                """
            exception "Can not change STRUCT to ARRAY"
        }
        // test struct to map
        test {
            sql """ alter table ${tableName} modify column c_s map<varchar(3),varchar(3)>
                """
            exception "Can not change STRUCT to MAP"
        }


        sql """ alter table ${tableName} modify column c_a array<varchar(20)> """
        int max_try_secs = 300
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });

        String[][] res = sql """ desc ${tableName} """
        logger.info(res[1][1])
        // array varchar(10)
        assertEquals(res[1][1].toLowerCase(),"array<varchar(20)>")

        qt_sc_origin " select * from ${tableName} order by c0; "

        // then insert some data to modified array with varchar(20)
        sql """ insert into ${tableName} values
                (2,['2025-01-03-22-33'], {'doris':'better'}, named_struct('col','amory'));
            """
        qt_sc_after " select * from ${tableName} order by c0; "

        // map varchar(10)
        sql """ alter table ${tableName} modify column c_m map<varchar(20),varchar(20)> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });

        res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"map<varchar(20),varchar(20)>")

        // insert some data to modified map with varchar(20)
        qt_sc_origin " select * from ${tableName} order by c0; "
        sql """ insert into ${tableName} values
                (3,['2025-01-03-22-33'], {'doris111111111':'better2222222222'}, named_struct('col','amory'));
            """
        qt_sc_after " select * from ${tableName} order by c0; "

        // struct varchar(10)
        sql """ alter table ${tableName} modify column c_s struct<col:varchar(20)> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });

        res = sql """ desc ${tableName} """
        logger.info(res[3][1])
        assertEquals(res[3][1].toLowerCase(),"struct<col:varchar(20)>")

        // insert some data to modified struct with varchar(20)
        qt_sc_origin " select * from ${tableName} order by c0; "
        sql """ insert into ${tableName} values
                (4,['2025-01-03-22-33'], {'doris':'better'}, named_struct('col','amoryIsBetter'));
            """
        qt_sc_after " select * from ${tableName} order by c0; "
    } finally {
         try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

}
