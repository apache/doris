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

suite("join_on_view") {
    sql """
        drop table if exists jov_t1;
    """
    sql """
        drop table if exists jov_t2;
    """
    sql """
        CREATE TABLE jov_t1 (
        id int(11) NOT NULL COMMENT ''
        ) ENGINE=OLAP
        UNIQUE KEY(id)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    CREATE TABLE jov_t2 (
    name varchar(128) COMMENT ''
    ) ENGINE=OLAP
    UNIQUE KEY(name)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(name) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    
    qt_sql """
        SELECT cd
        FROM
        (SELECT CURDATE() cd
        FROM jov_t1) tbl1
        JOIN
        (select cast(now() as string) td
        from jov_t2 b
        GROUP BY now()) tbl2
        ON tbl1.cd = tbl2.td;
    """

    sql """
        drop table jov_t1;
    """
    sql """
        drop table jov_t2;
    """
}