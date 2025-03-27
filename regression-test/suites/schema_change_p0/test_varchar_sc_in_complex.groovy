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
        def exception_str = isGroupCommitMode() ? "too many filtered rows" : "Insert has filtered data in strict mode"
        test {
            sql """ insert into ${tableName} values
                (11, ['2025-01-03-22-33'], {'doris111111111':'better2222222222'}, named_struct('col','amoryIsBetter'));
            """
            exception exception_str
        }

        // case1. can not alter modify column to shorten string length for array/map/struct
        test {
            sql """ alter table ${tableName} modify column c_a array<varchar(3)>
                """
            exception "Shorten type length is prohibited"
        }

        test {
            sql """ alter table ${tableName} modify column c_m map<varchar(3),varchar(3)>
                """
            exception "Shorten type length is prohibited"
        }

        test {
            sql """ alter table ${tableName} modify column c_s struct<col:varchar(3)>
                """
            exception "Shorten type length is prohibited"
        }

        // case2, can not alter modify array/map/struct to other type
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

        // case3. can alter modify column to enlarge string length for array/map/struct
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

        // case4. rename origin column , then alter modify column to enlarge string length for array/map/struct
        // rename
        sql """ alter table ${tableName} rename column c_a c_a_new """
        res = sql """ desc ${tableName} """
        logger.info(res[1][0])
        logger.info(res[1][1])
        assertEquals(res[1][0].toLowerCase(),"c_a_new")
        assertEquals(res[1][1].toLowerCase(),"array<varchar(20)>")

        // insert data
        sql """ insert into ${tableName} values
                (5,['2025-01-03-22-33'], {'doris':'better'}, named_struct('col','amory'));
            """
        // check data
        qt_sc_origin " select * from ${tableName} where c0 = 5; "
        // modify
        sql """ alter table ${tableName} modify column c_a_new array<varchar(30)> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // insert data
        sql """ insert into ${tableName} values
                (6,['2025-01-03-22-33:11111111111111111'], {'doris':'better'}, named_struct('col','amory'));
            """
        // check data
        qt_sc_after " select * from ${tableName} where c0 = 6; "


        sql """ alter table ${tableName} rename column c_m c_m_new """
        res = sql """ desc ${tableName} """
        logger.info(res[2][0])
        assertEquals(res[2][0].toLowerCase(),"c_m_new")

        // insert data
        sql """ insert into ${tableName} values
                (7,['2025-01-03-22-33'], {'doris111111111':'better2222222222'}, named_struct('col','amory'));
            """
        // check data
        qt_sc_origin " select * from ${tableName} where c0 = 7; "
        // modify
        sql """ alter table ${tableName} modify column c_m_new map<varchar(30),varchar(30)> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // insert data
        sql """ insert into ${tableName} values
                (8,['2025-01-03-22-33'], {'doris1234567890dorisdorisdoris1222':'better1234567890betterbetter12345678'}, named_struct('col','amory'));
            """
        // check data
        qt_sc_after " select * from ${tableName} where c0 = 8; "

        sql """ alter table ${tableName} rename column c_s c_s_new """
        res = sql """ desc ${tableName} """
        logger.info(res[3][0])
        assertEquals(res[3][0].toLowerCase(),"c_s_new")

        // insert data
        sql """ insert into ${tableName} values
                (9,['2025-01-03-22-33'], {'doris':'better'}, named_struct('col','amoryIsBetter'));
            """
        // check data
        qt_sc_origin " select * from ${tableName} where c0 = 9; "
        // modify
        sql """ alter table ${tableName} modify column c_s_new struct<col:varchar(30)> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // insert data
        sql """ insert into ${tableName} values
                (10,['2025-01-03-22-33'], {'doris':'better'}, named_struct('col','amoryIsBetteramoryIsBetteramor'));
                """
        // check data
        qt_sc_after " select * from ${tableName} where c0 = 10; "


        // case5. create table for 3-level nested type
        //        array<array<array<string>>>;
        //        array<map<string, array<string>>>;
        //        map<string, array<map<string, string>>>;
        //        map<string, map<string, array<string>>>;
        //        struct<col:array<map<string, string>>, col2:map<string, array<string>>>;
        sql """ DROP TABLE IF EXISTS there_level_nested_type """
        sql """
            CREATE TABLE IF NOT EXISTS there_level_nested_type (
                `c0` LARGEINT NOT NULL,
                `c_a` ARRAY<ARRAY<ARRAY<VARCHAR(10)>>>,
                `c_b` ARRAY<MAP<VARCHAR(10),ARRAY<VARCHAR(10)>>>,
                `c_c` MAP<VARCHAR(10),ARRAY<MAP<VARCHAR(10),VARCHAR(10)>>>,
                `c_d` MAP<VARCHAR(10),MAP<VARCHAR(10),ARRAY<VARCHAR(10)>>>,
                `c_s` STRUCT<col:ARRAY<MAP<VARCHAR(10),VARCHAR(10)>>, col2:MAP<VARCHAR(10),ARRAY<VARCHAR(10)>>>
            ) DISTRIBUTED BY HASH(c0) BUCKETS 1
            PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" )
        """

        // insert data
        sql """ insert into there_level_nested_type values
            (0,
             [[['2025-01-01'], ['2025-01-02']], [['2025-01-03'], ['2025-01-04']]],
             [{'key1': ['value1', 'value2']}, {'key2': ['value3', 'value4']}],
             {'key1': [{'subkey1': 'subvalue1'}, {'subkey2': 'subvalue2'}]},
             {'key1': {'subkey1': ['subvalue1', 'subvalue2']}},
             named_struct('col', [{'key1': 'value1'}, {'key2': 'value2'}], 'col2', {'key1': ['value1', 'value2']})
            ),
            (1,
             [[['2025-02-01'], ['2025-02-02']], [['2025-02-03'], ['2025-02-04']]],
             [{'key3': ['value5', 'value6']}, {'key4': ['value7', 'value8']}],
             {'key2': [{'subkey3': 'subvalue3'}, {'subkey4': 'subvalue4'}]},
             {'key2': {'subkey2': ['subvalue3', 'subvalue4']}},
             named_struct('col', [{'key3': 'value3'}, {'key4': 'value4'}], 'col2', {'key2': ['value3', 'value4']})
            ),
            (2,
             [[['2025-03-01'], ['2025-03-02']], [['2025-03-03'], ['2025-03-04']]],
             [{'key5': ['value9', 'value10']}, {'key6': ['value11', 'value12']}],
             {'key3': [{'subkey5': 'subvalue5'}, {'subkey6': 'subvalue6'}]},
             {'key3': {'subkey3': ['subvalue5', 'subvalue6']}},
             named_struct('col', [{'key5': 'value5'}, {'key6': 'value6'}], 'col2', {'key3': ['value5', 'value6']})
            )
            """
        qt_sc_origin " select * from there_level_nested_type order by c0; "
        // modify
        sql """ alter table there_level_nested_type modify column c_a array<array<array<varchar(20)>>> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState("there_level_nested_type")
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // check column type
        res = sql """ desc there_level_nested_type """
        logger.info(res[1][1])
        assertEquals(res[1][1].toLowerCase(),"array<array<array<varchar(20)>>>")
        // insert data
        sql """ insert into there_level_nested_type values
            (3,
             [[['2025-04-01 00:00'], ['2025-04-02 00:00']], [['2025-04-03 00:00'], ['2025-04-04 00:00']]],
             [{'key7': ['value13', 'value14']}, {'key8': ['value15', 'value16']}],
             {'key4': [{'subkey7': 'subvalue7'}, {'subkey8': 'subvalue8'}]},
             {'key4': {'subkey4': ['subvalue7', 'subvalue8']}},
             named_struct('col', [{'key7': 'value7'}, {'key8': 'value8'}], 'col2', {'key4': ['value7', 'value8']})
            )
            """
        qt_sc_after " select * from there_level_nested_type order by c0; "
        // modify
        sql """ alter table there_level_nested_type modify column c_b array<map<varchar(20),array<varchar(20)>>> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState("there_level_nested_type")
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // check column type
        res = sql """ desc there_level_nested_type """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"array<map<varchar(20),array<varchar(20)>>>")
        // insert data
        sql """ insert into there_level_nested_type values
            (4,
             [[['2025-05-01 00:00'], ['2025-05-02 00:00']], [['2025-05-03 00:00'], ['2025-05-04 00:00']]],
             [{'key1234567890': ['value1234567890', 'value1234567890']}, {'key1234567890': ['value1234567890', 'value1234567890']}],
             {'key5': [{'subkey9': 'subvalue9'}, {'subkey10': 'subvalue10'}]},
             {'key5': {'subkey5': ['subvalue9', 'subvalue10']}},
             named_struct('col', [{'key9': 'value9'}, {'key10': 'value10'}], 'col2', {'key5': ['value9', 'value10']})
            )
            """
        qt_sc_after " select * from there_level_nested_type order by c0; "
        // modify
        sql """ alter table there_level_nested_type modify column c_c map<varchar(20),array<map<varchar(20),varchar(20)>>> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState("there_level_nested_type")
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // check column type
        res = sql """ desc there_level_nested_type """
        logger.info(res[3][1])
        assertEquals(res[3][1].toLowerCase(),"map<varchar(20),array<map<varchar(20),varchar(20)>>>")
        // insert data
        sql """ insert into there_level_nested_type values
            (5,
             [[['2025-06-01 00:00'], ['2025-06-02 00:00']], [['2025-06-03 00:00'], ['2025-06-04 00:00']]],
             [{'key1234567890': ['value1234567890', 'value1234567890']}, {'key1234567890': ['value1234567890', 'value1234567890']}],
             {'key1234567890': [{'subkey11': 'subvalue11'}, {'subkey12': 'subvalue12'}]},
             {'key6': {'subkey6': ['subvalue11', 'subvalue12']}},
             named_struct('col', [{'key11': 'value11'}, {'key12': 'value12'}], 'col2', {'key6': ['value11', 'value12']})
            )
            """
        qt_sc_after " select * from there_level_nested_type order by c0; "
        // modify
        sql """ alter table there_level_nested_type modify column c_d map<varchar(20),map<varchar(20),array<varchar(20)>>> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState("there_level_nested_type")
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // check column type
        res = sql """ desc there_level_nested_type """
        logger.info(res[4][1])
        assertEquals(res[4][1].toLowerCase(),"map<varchar(20),map<varchar(20),array<varchar(20)>>>")
        // insert data
        sql """ insert into there_level_nested_type values
            (6,
             [[['2025-07-01 00:00'], ['2025-07-02 00:00']], [['2025-07-03 00:00'], ['2025-07-04 00:00']]],
             [{'key1234567890': ['value1234567890', 'value1234567890']}, {'key1234567890': ['value1234567890', 'value1234567890']}],
             {'key1234567890': [{'subkey13': 'subvalue13'}, {'subkey14': 'subvalue14'}]},
             {'key1234567890': {'subkey7': ['subvalue13', 'subvalue14']}},
             named_struct('col', [{'key13': 'value13'}, {'key14': 'value14'}], 'col2', {'key7': ['value13', 'value14']})
            )
            """
        qt_sc_after " select * from there_level_nested_type order by c0; "
        // modify
        sql """ alter table there_level_nested_type modify column c_s struct<col:array<map<varchar(20),varchar(20)>>,col2:map<varchar(20),array<varchar(20)>>> """
        Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState("there_level_nested_type")
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });
        // check column type
        res = sql """ desc there_level_nested_type """
        logger.info(res[5][1])
        assertEquals(res[5][1].toLowerCase(),"struct<col:array<map<varchar(20),varchar(20)>>,col2:map<varchar(20),array<varchar(20)>>>")
        // insert data
        sql """ insert into there_level_nested_type values
            (7,
             [[['2025-08-01 00:00'], ['2025-08-02 00:00']], [['2025-08-03 00:00'], ['2025-08-04 00:00']]],
             [{'key1234567890': ['value1234567890', 'value1234567890']}, {'key1234567890': ['value1234567890', 'value1234567890']}],
             {'key1234567890': [{'subkey15': 'subvalue15'}, {'subkey16': 'subvalue16'}]},
             {'key1234567890': {'subkey8': ['subvalue15', 'subvalue16']}},
             named_struct('col', [{'key1234567890': 'value1234567890'}, {'key1234567890': 'value1234567890'}], 'col2', {'key1234567890': ['value1234567890', 'value1234567890']})
            )
            """
        // check data
        qt_sc_after " select * from there_level_nested_type order by c0; "

        // case6. we do not support create mv for complex type
        // create mv for column and then alter modify column to enlarge string length for array/map/struct
//        def mvName = "mv_there_level_nested_type"
//        def dbName = "regression_test_schema_change_p0"
//        def querySql =  "select c_a, c_b, c_c from there_level_nested_type"
//        sql """ create materialized view ${mvName} as select c0, c_a, c_b, c_c from there_level_nested_type """
//        def jobName = getJobName("there_level_nested_type", "mv_there_level_nested_type")
//        order_qt_init "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
//        sql """
//        REFRESH MATERIALIZED VIEW ${mvName} AUTO
//        """
//        waitingMTMVTaskFinished(jobName)
//        order_qt_success "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
//
//        mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")
//
//        // then alter modify column to enlarge string length for array/map/struct
//        sql """ alter table there_level_nested_type modify column c_a array<array<array<varchar(30)>>> """
//        assertEquals("FINISHED", getAlterColumnFinalState("there_level_nested_type"))
//        order_qt_alter_column "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
//        mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")
//        sql """ alter table there_level_nested_type modify column c_b array<map<varchar(30),array<varchar(30)>>> """
//        assertEquals("FINISHED", getAlterColumnFinalState("there_level_nested_type"))
//        order_qt_alter_column "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
//        mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")
//        sql """ alter table there_level_nested_type modify column c_c map<varchar(30),array<map<varchar(30),varchar(30)>>> """
//        assertEquals("FINISHED", getAlterColumnFinalState("there_level_nested_type"))
//        order_qt_alter_column "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
//
//        // insert data
//        sql """ insert into there_level_nested_type values
//            (8,
//             [[['2025-09-01 00:0011111111'], ['2025-09-02 00:00111111']], [['2025-09-03 00:0011111'], ['2025-09-04 00:001111']]],
//             [{'key123456789011111111': ['value123456789011111', 'value1234567890']}, {'key1234567890111111111': ['value1234567890', 'value1234567890111111']}],
//             {'key1234567890111111111': [{'subkey171111111111111': 'subvalue17'}, {'subkey181111111111111111': 'subkey181111111111111111'}]},
//             {'key1234567890': {'subkey9': ['subvalue17', 'subvalue18']}},
//             named_struct('col', [{'key17': 'value17'}, {'key18': 'value18'}], 'col2', {'key9': ['value17', 'value18']})
//            )
//            """
//        qt_sc_after " select * from there_level_nested_type order by c0; "

        // case7. do not support enlarge char length in nested types
        def tableName2 = "test_enlarge_char_length_nested"
        sql """ DROP TABLE IF EXISTS ${tableName2} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2}
            (
                k BIGINT NOT NULL,
                c1 ARRAY<CHAR(10)>,
                c2 MAP<CHAR(10), CHAR(10)>,
                c3 STRUCT<col:CHAR(10)>,
                c4 ARRAY<VARCHAR(10)>,
                c5 MAP<VARCHAR(10), VARCHAR(10)>,
                c6 STRUCT<col:VARCHAR(10)>
            ) DISTRIBUTED BY HASH(k) BUCKETS 1
              PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" );
        """
        test {
            sql """ ALTER TABLE ${tableName2} MODIFY COLUMN c1 ARRAY<CHAR(20)> """
            exception "Cannot change char(10) to char(20) in nested types"
        }
        test {
            sql """ ALTER TABLE ${tableName2} MODIFY COLUMN c2 MAP<CHAR(20), CHAR(20)> """
            exception "Cannot change char(10) to char(20) in nested types"
        }
        test {
            sql """ ALTER TABLE ${tableName2} MODIFY COLUMN c3 STRUCT<col:CHAR(20)> """
            exception "Cannot change char(10) to char(20) in nested types"
        }

        // case8. do not support convert from char to varchar and varchar to char in nested types
        test {
            sql """ ALTER TABLE ${tableName2} MODIFY COLUMN c1 ARRAY<VARCHAR(20)> """
            exception "Cannot change char(10) to varchar(20) in nested types"
        }
        test {
            sql """ ALTER TABLE ${tableName2} MODIFY COLUMN c2 MAP<VARCHAR(20), VARCHAR(20)> """
            exception "Cannot change char(10) to varchar(20) in nested types"
        }
        test {
            sql """ ALTER TABLE ${tableName2} MODIFY COLUMN c3 STRUCT<col:VARCHAR(20)> """
            exception "Cannot change char(10) to varchar(20) in nested types"
        }

        test {
            sql """ ALTER TABLE ${tableName2} MODIFY COLUMN c4 ARRAY<CHAR(20)> """
            exception "Cannot change varchar(10) to char(20) in nested types"
        }
        test {
            sql """ ALTER TABLE ${tableName2} MODIFY COLUMN c5 MAP<CHAR(20), CHAR(20)> """
            exception "Cannot change varchar(10) to char(20) in nested types"
        }
        test {
            sql """ ALTER TABLE ${tableName2} MODIFY COLUMN c6 STRUCT<col:CHAR(20)> """
            exception "Cannot change varchar(10) to char(20) in nested types"
        }
    } finally {
         try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
