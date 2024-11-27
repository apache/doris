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

suite("big_join_build") {

    sql """ DROP TABLE IF EXISTS b_table; """
    sql """ DROP TABLE IF EXISTS p_table; """

    sql """
            create table b_table (
                k1 tinyint not null,
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 64
            properties("replication_num" = "1");
        """
    sql """
            create table p_table (
                k1 tinyint not null,
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 64
            properties("replication_num" = "1");
        """
    sql """
    insert into p_table select * from numbers("number" = "5");
    """
    sql """
    insert into b_table select * from numbers("number" = "1000000000");
    """
    sql """
    insert into b_table select * from numbers("number" = "1000000000");
    """
    sql """
    insert into b_table select * from numbers("number" = "1000000000");
    """
    sql """
    insert into b_table select * from numbers("number" = "1000000000");
    """
    sql """
    insert into b_table select * from numbers("number" = "1000000000");
    """

    qt_sql"""select /*+ leading(p_table b_table) */ count(*) from p_table,b_table where p_table.k1=b_table.k1 and b_table.k1<91;"""
}

