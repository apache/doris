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

suite("show_view_command") {
    multi_sql """
        drop database if exists test_show_view_db1;
        create database test_show_view_db1;
        use test_show_view_db1;

        drop table if exists t1;
        drop table if exists t2;

        create table t1
                (c1 bigint, c2 bigint)
                ENGINE=OLAP
        DUPLICATE KEY(c1, c2)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );

        create table t2
                (c1 bigint, c2 bigint)
                ENGINE=OLAP
        DUPLICATE KEY(c1, c2)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );

        drop view if exists tv1;
        drop view if exists tv2;

        create view tv1 as select t1.c1, t2.c2 from t1 join t2 on t1.c2 = t2.c2;
        create view tv2 as select t1.c1 from t1 where t1.c2 = 1;

        drop database if exists test_show_view_db2;
        create database test_show_view_db2;
        use test_show_view_db2;

        drop table if exists t1;
        drop table if exists t2;

        create table t1
                (c1 bigint, c2 bigint)
                ENGINE=OLAP
        DUPLICATE KEY(c1, c2)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );

        create table t2
                (c1 bigint, c2 bigint)
                ENGINE=OLAP
        DUPLICATE KEY(c1, c2)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );

        drop view if exists ttv1;
        drop view if exists ttv2;

        create view ttv1 as select t1.c1, t2.c2 from t1 join t2 on t1.c2 = t2.c2;
        create view ttv2 as select t1.c1 from t1 where t1.c2 = 1;
    """
    checkNereidsExecute("""show view from t1;""")
    checkNereidsExecute("""show view from test_show_view_db1.t1;""")
    checkNereidsExecute("""show view from test_show_view_db2.t1;""")
    checkNereidsExecute("""show view from test_show_view_db1.t1 from test_show_view_db2;""")
    qt_cmd("""show view from t1;""")
    qt_cmd("""show view from test_show_view_db1.t1;""")
    qt_cmd("""show view from test_show_view_db2.t1;""")
    qt_cmd("""show view from test_show_view_db1.t1 from test_show_view_db2;""")
}