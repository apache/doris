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

suite("no_null_str_rf") {
    sql """ DROP TABLE IF EXISTS d_table; """
    sql """ DROP TABLE IF EXISTS dd_table; """
    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) not null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 1,1,1,'b';"
    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 1,1,1,'b';"
    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 1,1,1,'b';"
    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 1,1,1,'b';"

    sql """
            create table dd_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) not null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into dd_table select 1,1,1,'a';"
    sql "insert into dd_table select 1,1,1,'c';"

    qt_test """select count(1) from d_table,dd_table where d_table.k4=dd_table.k4 and d_table.k1=1 and dd_table.k1=1;"""
}
