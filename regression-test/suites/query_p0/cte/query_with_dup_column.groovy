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

import org.junit.Assert;

suite("query_with_dup_column") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists test_table;
    """

    sql """
    CREATE TABLE `test_table` (
      `unique_id`    varchar(256) NULL,
      `name` varchar(256) NULL
    )
    PROPERTIES (
        "replication_num" = "1"
    );
    """

    sql """
    insert into test_table values ("yyyxxxzzz", "abc000000")
    """

    // should fail
    try {
    sql """
    with tmp1 as (
    select unique_id, unique_id  from test_table
    )
    select * from tmp1;
    """
    } catch (Exception e) {
        assertTrue(e.message.contains("Duplicated inline view column alias"))
    }

    // should fail
    try {
    sql """
    with tmp1 as (
    select unique_id, unique_id  from test_table
    )
    select * from tmp1 t;
    """
    } catch (Exception e) {
        assertTrue(e.message.contains("Duplicated inline view column alias"))
    }


    try {
        sql """
    with tmp1 as (
    select *, unique_id from test_table
    )
    select * from tmp1;
    """
    } catch (Exception e) {
        assertTrue(e.message.contains("Duplicated inline view column alias"))
    }

    // should fail
    try {
    sql """
    with tmp1 as (
    select *, unique_id from test_table
    )
    select * from tmp1 t;
    """
    } catch (Exception e) {
        assertTrue(e.message.contains("Duplicated inline view column alias"))
    }

    // should success
    sql """
    select *, unique_id from test_table;
    """

    // should success
    sql """
    select *, unique_id from test_table t;
    """

    // should success
    sql """
    select unique_id, unique_id from test_table
    """

    // should success
    sql """
    select unique_id, unique_id from test_table t
    """
}

