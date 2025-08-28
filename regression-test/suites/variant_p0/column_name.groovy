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

suite("regression_test_variant_column_name", "variant_type"){
    def table_name = "var_column_name"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1 
        properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """ 

    sql """insert into ${table_name} values (1, '{"中文" : "中文", "\\\u4E2C\\\u6587": "unicode"}')"""
    qt_sql """select v['中文'], v['\\\u4E2C\\\u6587'] from ${table_name}"""
    // sql """insert into ${table_name} values (2, '{}')"""
    sql "truncate table ${table_name}"
    sql """insert into ${table_name} values (3, '{"": ""}')"""
    qt_sql """select cast(v[''] as text) from ${table_name} order by k"""
    sql """insert into ${table_name} values (4, '{"!@#^&*()": "11111"}')"""
    qt_sql """select cast(v["!@#^&*()"] as string) from ${table_name} order by k"""
    sql """insert into ${table_name} values (5, '{"123": "456", "789": "012"}')"""
    qt_sql """select cast(v["123"] as string) from ${table_name} order by k"""
    // sql """insert into ${table_name} values (6, '{"\\n123": "t123", "\\\"123": "123"}')"""
    sql """insert into ${table_name} values (7, '{"AA": "UPPER CASE", "aa": "lower case"}')"""
    qt_sql """select cast(v["AA"] as string), cast(v["aa"] as string) from ${table_name} order by k"""

    sql "alter table var_column_name rename column v Tags;  "
    sql """insert into var_column_name values (1, '{"tag_key1" : 123456}')"""
    qt_sql "select * from var_column_name where tags['tag_key1'] is not null and cast(Tags['tag_key1' ] as text) = '123456' order by k desc limit 1;"    
    qt_sql "select * from var_column_name where tags['tag_key1'] is not null and cast(tags['tag_key1' ] as text) = '123456' order by k desc limit 1;"    
    qt_sql "select * from var_column_name where Tags['tag_key1'] is not null and cast(tags['tag_key1' ] as text) = '123456' order by k desc limit 1;"    
    qt_sql "select * from var_column_name where Tags['tag_key1'] is not null and cast(Tags['tag_key1' ] as text) = '123456' order by k desc limit 1;"    

    // empty key
    sql """insert into var_column_name values (7, '{"": "UPPER CASE"}')"""
    sql """
        insert into var_column_name values (7, '{"":16,"OpenCapStatus":0,"AccStatus":1,"AccTimeSum":481,"LowVoltage":0,"TowedStatus":0,"EncryptLng":117.23572361077638,"deviceId":"A1100614808888"}')
    """
    sql """insert into var_column_name values (7, '{"": ""}')"""
    sql """insert into var_column_name values (7, '{"": "dkdkdkdkdkd"}')"""
    sql """insert into var_column_name values (7, '{"": "xmxxmmmmmm"}')"""
    sql """insert into var_column_name values (7, '{"": "ooaoaaaaaaa"}')"""
    sql """insert into var_column_name values (7, '{"": 1234566}')"""
    sql """insert into var_column_name values (7, '{"": 8888888}')"""

    qt_sql "select cast(Tags[''] as text) from var_column_name order by cast(Tags[''] as string)"

    // name with `.`
    sql "truncate table var_column_name"
    test {
        sql """insert into var_column_name values (7, '{"a.b": "UPPER CASE", "a.c": "lower case", "a" : {"b" : 123}, "a" : {"c" : 456}}')"""
        exception "may contains duplicated entry"
    }
    for (int i = 0; i < 7; i++) {
        sql """insert into var_column_name select * from var_column_name"""
    }
    qt_sql_cnt_1 "select count(Tags['a.b']) from var_column_name"
    qt_sql_cnt_2 "select count(Tags['a.c']) from var_column_name"
    qt_sql_cnt_3 "select count(Tags['a']['b']) from var_column_name"
    qt_sql_cnt_4 "select count(Tags['a']['c']) from var_column_name"

    try {
        sql """insert into var_column_name values (7, '{"": "UPPER CASE", "": "lower case"}')"""
    } catch(Exception ex) {
        logger.info("""INSERT INTO ${table_name} failed: """ + ex)
        assertTrue(ex.toString().contains("may contains duplicated entry"));
    }

    // test key length larger than 255 bytes
    def key = "a"
    for (int i = 0; i < 256; i++) {
        key += "a"
    }
    test {
        sql """insert into var_column_name values (8, '{"${key}": "test"}')"""
        exception "Key length exceeds maximum allowed size of 255 bytes."
    }
}