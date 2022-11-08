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

suite("test_join_with_projection") {
    sql """
        drop table if exists test_join_with_projection_outerjoin_A;
    """

    sql """
        drop table if exists test_join_with_projection_outerjoin_B;
    """

    sql """
        create table if not exists test_join_with_projection_outerjoin_A ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists test_join_with_projection_outerjoin_B ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into test_join_with_projection_outerjoin_A values( 1 );
    """

    sql """
        insert into test_join_with_projection_outerjoin_B values( 1 );
    """

    qt_select """
        select
            CASE
            WHEN true THEN
            1
            ELSE 1
            END AS c0
        FROM 
            (SELECT a AS c0
            FROM test_join_with_projection_outerjoin_A
            ) AS subq_1
        RIGHT JOIN 
            (SELECT a AS c0
            FROM test_join_with_projection_outerjoin_B
            ) AS subq_2
            ON (subq_1.c0 = subq_2.c0 );
    """

    sql """
        drop table if exists test_join_with_projection_outerjoin_A;
    """

    sql """
        drop table if exists test_join_with_projection_outerjoin_B;
    """
}
