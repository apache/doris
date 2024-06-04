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

suite("lateral_view_with_inline_view") {
    sql """DROP TABLE IF EXISTS t1;"""
    sql """CREATE TABLE t1 (col1 varchar(11451) not null, col2 int not null, col3 int not null, col4 int not null)
    UNIQUE KEY(`col1`)
    DISTRIBUTED BY HASH(col1)
    BUCKETS 3
    PROPERTIES(
            "replication_num"="1"
    );"""
    qt_sql """
    select example1.col1
    from 
    (select col1,`col4`,col2
    from t1 where col3<=115411) 
    example1 lateral view explode([1,2,3]) tmp1 as e1 where col4<e1;
    """

    qt_sql """
    select
        e1 hour,
        sum(money)
    from
        (
            select
                *
            from
                (
                    select
                        1 hour,
                        10 money
                    union
                    all
                    select
                        3 hour,
                        10 money
                ) t lateral view explode([23,24]) tmp1 as e1
            where
                hour <= e1
        ) tt
    group by 1
    order by 1;
    """
    sql """create view xx_lateral as select example1.col1 from (select col1,`col4`,col2 from t1 where col3<=115411) example1 lateral view explode([1,2,3]) tmp1 as `e1 aa`;"""
    sql """drop view xx_lateral"""
}