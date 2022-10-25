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

suite("test_union_with_subquery") {
    sql """ DROP TABLE IF EXISTS A_union; """
    sql """
        create table if not exists A_union ( a int not null, b varchar(10)  null )ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """ insert into A_union values( 1, '1' ); """
    
    qt_sql """with c AS 
                (SELECT a,
                    b
                FROM A_union
                GROUP BY  b, a ), d AS 
                (SELECT a,
                    b
                FROM A_union
                GROUP BY  b, a )
                SELECT *
                FROM d
                UNION
                SELECT *
                FROM c;
    """

    sql """ DROP TABLE IF EXISTS A_union; """
}
