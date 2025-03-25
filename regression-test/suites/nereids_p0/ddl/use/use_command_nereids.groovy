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

suite("use_command_nereids") {
    String db1 = "test_use_command_db1"
    String db2 = "test_use_command_db2"
    String tbl1 = "tb1"
    String tbl2 = "tb2"

    sql """drop database if exists ${db1};"""
    sql """drop database if exists ${db2};"""
    // create database
    sql """create database ${db1};"""
    sql """create database ${db2};"""
    //cloud-mode
    if (isCloudMode()) {
        return
    }
    // use command
    checkNereidsExecute("use ${db1};")

    """drop table if exists ${tbl1};"""
    sql """ create table ${db1}.${tbl1}
           (
            c1 bigint,
            c2 bigint
           )
           ENGINE=OLAP
           DUPLICATE KEY(c1, c2)
           COMMENT 'OLAP'
           DISTRIBUTED BY HASH(c1) BUCKETS 1
           PROPERTIES (
           "replication_num" = "1"
           );
    """
    qt_show_tables_db1 """show tables;"""

    checkNereidsExecute("use ${db2};")
    """drop table if exists ${tbl2};"""
    sql """ create table ${db2}.${tbl2}
               (
                c1 bigint,
                c2 bigint
               )
               ENGINE=OLAP
               DUPLICATE KEY(c1, c2)
               COMMENT 'OLAP'
               DISTRIBUTED BY HASH(c1) BUCKETS 1
               PROPERTIES (
               "replication_num" = "1"
               );
        """

    qt_show_tables_db2 """show tables;"""

    checkNereidsExecute("use internal.${db1};")
    qt_show_tables_db1 """show tables;"""
    checkNereidsExecute("use internal.${db2};")
    qt_show_tables_db2 """show tables;"""

    sql """drop table if exists ${db1}.${tbl1};"""
    sql """drop table if exists ${db2}.${tbl2};"""
    sql """drop database if exists ${db1};"""
    sql """drop database if exists ${db2};"""
}