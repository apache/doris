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

suite("test_column_resize") {
    sql "drop table if exists t1;"
    sql "drop table if exists t2;"

    sql """
        CREATE TABLE `t1` (
                `id` int NULL,
                `s` struct<a:int,b:text> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `t2` (
                `id` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
        ); 
    """

    sql """
        insert into t1 values(1, '{"a": 1, "b": "abc"}');
    """
    sql """
        insert into t2 values(1),(3),(5),(6),(7);
    """

    qt_sql_insert_default_instead_of_resize """
        select t2.id, t1.s from t2 left join t1 on t2.id = t1.id and t1.id >1 order by t2.id;
    """
}