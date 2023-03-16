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

suite("test_implict_cast") {
    sql """
        drop table if exists cast_test_table;
    """
    
    sql """
        create table cast_test_table ( a decimal(18, 3) not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into cast_test_table values( 1 );
    """

    qt_select """
        select
        round(
            sum(
            ifnull(
                case
                when a = 0 then a
                else 0
                end,
                0
            )
            ),
            2
        ) as a
        from
        cast_test_table;
    """

    sql """
        drop table if exists cast_test_table;
    """
}
