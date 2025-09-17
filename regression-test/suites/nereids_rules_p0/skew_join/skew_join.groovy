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

suite("skew_join") {
    sql """
        drop table if exists t1;
        create table t1 (
            id int,
            value int
        )
        properties("replication_num"="1");

        insert into t1 values (1, 1);

        drop table if exists t2;
        create table t2 (
            id int,
            value int
        )
        properties("replication_num"="1");

        insert into t2 values (1, 2);

        alter table t1 modify column id set stats ('row_count' = '1000000000', 'ndv'='10000', 'hot_values'='1 :0.8');

        set runtime_filter_mode='OFF';
    """


    // big right: salt join
    sql "alter table t2 modify column id set stats ('row_count' = '1000000');"
    qt_salt_shape "explain shape plan select * from t2 join t1 on t1.id=t2.id;"
    qt_salt_exe "select * from t2 join t1 on t1.id=t2.id;"

    // small right: bc join
    sql "alter table t2 modify column id set stats ('row_count' = '10000');"
    qt_bc_shape "explain shape plan select * from t2 join t1 on t1.id=t2.id;"
    qt_bc_exe "select * from t2 join t1 on t1.id=t2.id;"
}