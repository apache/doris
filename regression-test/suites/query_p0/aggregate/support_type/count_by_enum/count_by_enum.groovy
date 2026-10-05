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

suite("count_by_enum") {
    sql """
        drop table if exists count_by_enum_test;
    """
    sql """
    CREATE TABLE count_by_enum_test(
        id varchar(1024) NULL,
        f1 text REPLACE_IF_NOT_NULL NULL,
        f2 text REPLACE_IF_NOT_NULL NULL,
        f3 text REPLACE_IF_NOT_NULL NULL
    )
    AGGREGATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 3
    PROPERTIES ("replication_num" = "1");
    """
    sql """
    INSERT into count_by_enum_test (id, f1, f2, f3) values
        (1, "F", "10", "北京"),
        (2, "F", "20", "北京"),
        (3, "M", NULL, "上海"),
        (4, "M", NULL, "上海"),
        (5, "M", NULL, "广州");
    """
    qt_count_by_enum_f1 """select count_by_enum(f1) from count_by_enum_test group by id order by id;"""
    qt_count_by_enum_f2 """select count_by_enum(f2) from count_by_enum_test group by id order by id;"""
    qt_count_by_enum_f1_f2_f3 """select count_by_enum(f1,f2,f3) from count_by_enum_test group by id order by id;"""
    qt_count_by_enum_f1_f2_f3 """select count_by_enum(f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1) from count_by_enum_test group by id order by id;"""

    test {
        sql "select count_by_enum(f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1,f1) from count_by_enum_test group by id order by id;"
        exception ""
    }
}
