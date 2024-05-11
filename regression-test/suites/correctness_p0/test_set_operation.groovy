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

suite("test_set_operation") {

    sql """
        drop table if exists test_set_operation_A;
    """

    sql """
        drop table if exists test_set_operation_B;
    """

    sql """
        create table test_set_operation_A ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table test_set_operation_B ( b int )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(b) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into test_set_operation_A values(1),(2),(3);
    """

    sql """
        insert into test_set_operation_B values(1),(null);
    """

    qt_sql """
        SELECT *
        FROM ( 
            (SELECT t0.a AS one_uid
            FROM test_set_operation_A t0 intersect ( 
                (SELECT b AS one_uid
                FROM test_set_operation_B ) EXCEPT 
                    (SELECT t0.a AS one_uid
                    FROM test_set_operation_A t0
                    WHERE a = 3 ) ) ) ) c
                ORDER BY  one_uid;
    """
    
    qt_select1  """ (select 0) intersect (select null); """
}
