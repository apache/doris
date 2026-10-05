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


suite("test_cast_to_string_from_int") {
    def int_values = [
        "0", "-0", "-1", "1", "123", "-123", "2147483647", "-2147483648",
    ]
    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in int_values) {
            qt_cast_int_const """select "${test_str}", cast("${test_str}" as int), cast(cast("${test_str}" as int) as string);"""
        }
    }

    sql """
        drop table if exists test_cast_to_string_from_int;
    """
    sql """
        create table test_cast_to_string_from_int (
            k1 int,
            v1 int
        ) properties("replication_num" = "1");
    """
    def insert_sql_str_int = "insert into test_cast_to_string_from_int values "
    def index = 0
    for (test_str in int_values) {
        insert_sql_str_int += """(${index}, "${test_str}"), """
        index++
    }
    insert_sql_str_int = insert_sql_str_int[0..-3]
    sql insert_sql_str_int
    qt_sql_int_to_string """
        select k1, v1, cast(v1 as string) from test_cast_to_string_from_int order by k1;
    """

}
