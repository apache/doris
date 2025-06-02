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

suite("test_invalid_skip_load") {
    sql "drop table if exists test_duplicate_argument;"
    sql " drop dictionary if exists test_duplicate_argument; "
    sql """
        create table test_duplicate_argument(
            k0 int not null,    
            k1 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """ 
    sql """insert into test_duplicate_argument values (1, 'abc'), (1, 'def');"""  

    sql """
        create dictionary dic_test_duplicate_argument using test_duplicate_argument
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='0');
    """

    for (int _ = 0; _ < 30 ; _++)
    {
        try {
            sql "refresh dictionary dic_test_duplicate_argument"
            assert false
        } catch (Exception e) {
            if (e.getMessage().contains("INVALID_ARGUMENT")) {
                break;
            } else {
                logger.info("refresh dictionary dic_test_duplicate_argument failed: " + e.getMessage())
            }
        }
        assertTrue(_ < 30, "refresh dictionary dic_test_duplicate_argument failed")
        sleep(1000)
    }

    def get_time = { input ->
        def pattern = ~/(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/
        def matcher = (input =~ pattern)
        matcher.find()
        return matcher.group(1)
    }

    def result = (sql """ show dictionaries; """)[0][6]
    def result_time = get_time(result)

    sleep(10000)
    def result2 = (sql """ show dictionaries; """)[0][6]
    def result_time2 = get_time(result2)
    assertEquals(result_time, result_time2)

    sql "delete from test_duplicate_argument where k0 = 1;"
    sql """insert into test_duplicate_argument values (1, 'abc'), (2, 'def');"""
    sleep(5000) // avoid get data after delete and before insert
    waitAllDictionariesReady()
    qt_sql """
        select dict_get("regression_test_dictionary_p0.dic_test_duplicate_argument", "k1", 1),
        dict_get("regression_test_dictionary_p0.dic_test_duplicate_argument", "k1", 2)  ;
    """
}