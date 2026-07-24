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

suite("test_is_true_false_predicate", "arrow_flight_sql") {
    sql """drop table if exists test_is_true_false_predicate;"""
    sql """
        create table test_is_true_false_predicate (
            id int null,
            b boolean null,
            c string null
        )
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
        """
    sql """insert into test_is_true_false_predicate values (1, true, 'true'), (2, false, 'false'), (3, null, 'abc'), (4, null, null);"""

    order_qt_is_true """
        select id from test_is_true_false_predicate where b is true order by id;
        """
    order_qt_is_false """
        select id from test_is_true_false_predicate where b is false order by id;
        """
    order_qt_is_not_true """
        select id from test_is_true_false_predicate where b is not true order by id;
        """
    order_qt_is_not_false """
        select id from test_is_true_false_predicate where b is not false order by id;
        """
    order_qt_truth_table """
        select id, b, b is true, b is false, b is not true, b is not false, c, c is true, c is false, c is not true, c is not false
        from test_is_true_false_predicate order by id;
        """
}
