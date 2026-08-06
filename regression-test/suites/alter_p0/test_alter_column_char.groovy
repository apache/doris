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

suite('test_alter_column_char') {
    def tbl = 'test_alter_column_char'
    sql "DROP TABLE IF EXISTS ${tbl}"

    sql """
        CREATE TABLE ${tbl} (
            `k1` BIGINT NOT NULL,
            `v1` BIGINT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        );
    """

    sql """
        INSERT INTO ${tbl} VALUES (1,1),(2,2),(3,3)
    """
    sql """ SYNC """
    qt_sql1 """ select * from ${tbl} order by k1 """

    sql """
        ALTER TABLE ${tbl} add column v2 char(10) default 'a'
    """

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }
    qt_sql2 """ select * from ${tbl} order by k1 """

    sql """
        INSERT INTO ${tbl} VALUES (4,4,'b')
    """
    sql """ SYNC """
    qt_sql3 """ select * from ${tbl} order by k1 """
    qt_sql4 """ select * from ${tbl} where v2 = 'a' order by k1 """
}
