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

suite("describe_command") {
    multi_sql """
        drop database if exists describe_command;
        create database describe_command;
        use describe_command;

        drop table if exists t1;

        create table t1
                (c1 bigint, c2 bigint)
                ENGINE=OLAP
        DUPLICATE KEY(c1, c2)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    qt_cmd("""describe t1;""")
    qt_cmd("""describe describe_command.t1;""")
    qt_cmd("""describe internal.describe_command.t1;""")
    qt_cmd("""describe t1 all;""")
//    qt_cmd("""describe t1 partition t1;""")
//    qt_cmd("""describe t1 partition (t1);""")
    qt_cmd("""describe function backends();""")
    qt_cmd("""drop table if exists t1;""")
}
