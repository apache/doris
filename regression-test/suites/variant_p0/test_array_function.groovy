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

suite("test_variant_array_function", "p0") {
    sql """ set enable_nereids_planner=true;"""
    sql """ set enable_fallback_to_original_planner=false;"""
    def tableName = "test_variant_array_function"
    sql """
        drop table if exists ${tableName};
    """

    sql """
       CREATE TABLE IF NOT EXISTS ${tableName} (
              `id` INT(11) null COMMENT "",
              `var` variant  null
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
    insert into ${tableName} values
    (1, '{"a":[1, 2,3],"b": ["a", "b", "c"], "c": [1.1, 2.2, 3.3]}')
    """

    sql """
    insert into ${tableName} values
    (2, '{"a":[1, 2,3],"b": ["1", "2", "3"], "c": [1.1, 2.2, 3.3]}')
   """


   qt_sql """
       select array_min(cast(var['a'] as array<int>)), array_min(cast(var['b'] as array<string>)), array_max(cast(var['a'] as array<int>)), array_max(cast(var['b'] as array<string>)) from ${tableName} order by id;
   """

    qt_sql """
        select array_join(cast(var['a'] as array<int>), ',', 'replaced'), array_join(cast(var['b'] as array<string>), ',', 'replaced'), array_join(cast(var['c'] as array<double>), ',', 'replaced') from ${tableName} order by id;
    """

    qt_sql """
        select array_sum(cast(var['a'] as array<int>)), array_sum(cast(var['c'] as array<double>)) from ${tableName} order by id;
    """

    qt_sql """
        select array_avg(cast(var['a'] as array<int>)), array_avg(cast(var['c'] as array<double>)) from ${tableName} order by id;
    """
    
    qt_sql """
        select array_product(cast(var['a'] as array<int>)), array_product(cast(var['c'] as array<double>)) from ${tableName} order by id;
    """

    qt_sql """ select array_avg(cast(var['b'] as array<string>)) from ${tableName} order by id; """

    qt_sql """ select array_product(cast(var['b'] as array<string>)) from ${tableName} order by id; """
}
