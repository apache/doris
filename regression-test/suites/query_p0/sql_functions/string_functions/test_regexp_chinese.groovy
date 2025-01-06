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

suite("test_regexp_chinese") {
    sql "DROP TABLE IF EXISTS regexp_test_chinese;"
    sql """
        CREATE TABLE regexp_test_chinese (
            id int NULL DEFAULT "0",
            city varchar(50) NOT NULL DEFAULT ""
        ) DISTRIBUTED BY HASH(id) BUCKETS 5 properties("replication_num" = "1");
    """

    sql """
        INSERT INTO regexp_test_chinese VALUES(1, "上海"),(2, "深圳"),(3, "上海测试"), (4, "北京测试");
    """

    qt_sql_regexp """
        SELECT * FROM regexp_test_chinese WHERE city REGEXP "^上海｜^北京" ORDER BY id;
    """
}

