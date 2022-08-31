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

suite("aggregate_over_join") {
    sql "drop table if exists t1"
    sql "drop table if exists t2"
    sql "drop table if exists t3"

    sql """
        CREATE TABLE t1 (col1 int not null, col2 int not null, col3 int not null)
        DISTRIBUTED BY HASH(col3)
        BUCKETS 1
        PROPERTIES(
            "replication_num"="1"
        );
        """

    sql """insert into t1 values(1994, 1994, 1995)"""

    sql """
        CREATE TABLE t2 (col1 varchar null, col2 int null, col3 int null, col4 int null)
        DISTRIBUTED BY HASH(col3)
        BUCKETS 3
        PROPERTIES(
            "replication_num"="1"
        )
        """
    sql """insert into t2 values('1',1994,1994,1994)"""

    sql """
        CREATE TABLE t3 (col1 int, col2 int, col3 int, col4 boolean)
        DISTRIBUTED BY HASH(col3)
        BUCKETS 3
        PROPERTIES(
            "replication_num"="1"
        )
        """
    sql """
        insert into t3 values(1,1,1,1)
        """
    sql """
        insert into t3 values(1,1,6,1)
        """

    sql """
        insert into t3 values (1,2,6,1);

        """

    qt_select """
        SELECT col1
        FROM 
            (SELECT col1 FROM t1) a
        INNER JOIN 
            (SELECT sum(b3) AS 'b3u'
            FROM 
                (SELECT `c`.`col2` AS `c2`,
                 sum(b.col3) AS `b3`
                FROM `t2` b
                INNER JOIN `t3` c
                    ON ((`b`.`col1` = `c`.`col1`))
                GROUP BY  `c`.`col2`) dd ) b
        """
}