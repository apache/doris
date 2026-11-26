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

suite("test_insert_array_struct_function_signature") {
    sql "drop table if exists array_struct_function_signature"
    sql """
        create table array_struct_function_signature (
            id int,
            a array<struct<i: int, s: varchar(16)>>
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """

    sql "set enable_fast_analyze_into_values = true"
    sql """
        insert into array_struct_function_signature values (
            1,
            array(
                struct(100, repeat('x', 2)),
                struct(200, repeat('y', 2))
            )
        )
    """

    sql "set enable_fast_analyze_into_values = false"
    sql """
        insert into array_struct_function_signature values (
            2,
            array(
                struct(100, repeat('x', 2)),
                struct(200, repeat('y', 2))
            )
        )
    """

    order_qt_array_struct_function_signature "select * from array_struct_function_signature"
}
