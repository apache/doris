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

suite("try_expr_test") {
    sql "set debug_skip_fold_constant = true"


    sql """
         DROP TABLE IF EXISTS try_expr_strings;
    """

    sql """
        CREATE TABLE IF NOT EXISTS try_expr_strings (
        id int ,
        str VARCHAR
        )
        DISTRIBUTED BY HASH(str) BUCKETS 2
        PROPERTIES (
        "replication_num"="1"
        );
    """


    sql """
        INSERT INTO try_expr_strings VALUES
        (1, '123'),
        (2, '456'),
        (3, '789'),
        (4, 'abc'),
        (5, '12.34'),
        (6, NULL);
    """
    sql "set enable_strict_cast = false"

    qt_sql_no_strict_cast """ select id , str , try(cast(str as int) ) from try_expr_strings order by  id; """


    sql "set enable_strict_cast = true"

    qt_sql_strict_cast """ select id , str , try(cast(str as int) ) from try_expr_strings order by  id; """
}
