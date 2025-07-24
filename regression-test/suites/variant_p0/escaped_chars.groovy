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

suite("regression_test_variant_escaped_chars", "p0"){
    def tableName = "variant_escape_chars"

    sql """ DROP TABLE IF EXISTS variant_escape_chars """

    sql """
        CREATE TABLE IF NOT EXISTS variant_escape_chars (
            `id` INT,
            `description` VARIANT 
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'This is a test table with escape characters in description'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO variant_escape_chars VALUES
        (1, '{"a" : 123, "b" : "test with escape \\\\" characters"}'),
        (2, '{"a" : 456, "b" : "another test with escape \\\\\\\\ characters"}'),
        (3, '{"a" : 789, "b" : "test with single quote \\\' characters"}'),
        (4, '{"a" : 101112, "b" : "test with newline \\\\n characters"}'),
        (5, '{"a" : 131415, "b" : "test with tab \\\\t characters"}'),
        (6, '{"a" : 161718, "b" : "test with backslash \\\\b characters"}');
    """

    // test json value with escaped characters
    qt_select """ SELECT * FROM variant_escape_chars ORDER BY id """
    qt_select """ SELECT description['b'] FROM variant_escape_chars ORDER BY id """
    qt_select """ SELECT CAST(description['b'] AS TEXT) FROM variant_escape_chars ORDER BY id """

    sql """
        drop table if exists t01;
        create table t01(id int, b json, c json, d variant, e variant) properties ("replication_num" = "1");
        insert into t01 values (1, '{"c_json":{"a":"a\\\\nb"}}', '{"c_json": {"quote":"\\\\"Helvetica tofu try-hard gluten-free gentrify leggings.\\\\" - Remington Trantow"}}', '{"c_json": {"quote":"\\\\"Helvetica tofu try-hard gluten-free gentrify leggings.\\\\" - Remington Trantow"}}', '{"c_json":{"a":"a\\\\nb"}}');
    """
    qt_select """ SELECT * FROM t01 """
    qt_select """select json_extract(b, "\$.c_json"), e["c_json"] from t01;"""

    // test json keys with escaped characters, FIXED in 3.1.0
    // sql "truncate table variant_escape_chars"
    // sql """
    //     INSERT INTO variant_escape_chars VALUES
    //     (1, '{"test with escape \\\\" characters" : 123}'),
    //     (2, '{"another test with escape \\\\\\\\ characters" : 123}'),
    //     (3, '{"test with single quote \\\' characters" : 123}'),
    //     (4, '{"test with newline \\\\n characters":123}'),
    //     (5, '{"test with tab \\\\t characters" : 123}'),
    //     (6, '{"test with backslash \\\\b characters" : 123}');
    // """
    // qt_select """ SELECT * FROM variant_escape_chars ORDER BY id """
}
