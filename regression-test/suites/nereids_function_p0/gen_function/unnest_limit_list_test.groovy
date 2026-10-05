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

suite("unnest_limit_list_test", "unnest") {

    String prefix_str = "unnest_limit_list_"
    def tb_name1 = prefix_str + "table1"

    sql """drop table if exists ${tb_name1}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name1} (
            u_id INT,
            u_name VARCHAR(20),
            count_array ARRAY<INT> 
        ) 
        DUPLICATE KEY(u_id)
        DISTRIBUTED BY HASH(u_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """INSERT INTO ${tb_name1} VALUES (1, 'Alice', [1, 5, 10]);"""

    test {
        // Test that using UNNEST on a constant array directly in a LIMIT clause is invalid.
        sql """SELECT * FROM ${tb_name1} LIMIT UNNEST([1, 2]);"""
        exception "mismatched input"
    }

    test {
        // Test that using an alias from an UNNEST in the SELECT list within the LIMIT clause is invalid.
        sql """SELECT *, UNNEST(count_array) as cnt FROM ${tb_name1} LIMIT cnt;"""
        exception "mismatched input"
    }

    test {
        // Test that using a subquery containing UNNEST within the LIMIT clause is invalid.
        sql """SELECT * FROM ${tb_name1} LIMIT (SELECT val FROM UNNEST([5]) AS t(val));"""
        exception "mismatched input"
    }

    test {
        // Test that using UNNEST on a column directly in a LIMIT clause is invalid.
        sql """SELECT * FROM ${tb_name1} LIMIT UNNEST(count_array);"""
        exception "mismatched input"
    }

}
