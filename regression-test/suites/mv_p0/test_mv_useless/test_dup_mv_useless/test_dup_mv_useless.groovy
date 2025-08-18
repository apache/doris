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

suite ("test_dup_mv_useless") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    def testTable = "test_dup_mv_useless_table"
    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${testTable}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }
    sql """ DROP TABLE IF EXISTS ${testTable}; """

    sql """
            create table ${testTable} (
                k1 int null,
                k2 int null,
                k3 int null
            )
            duplicate key (k1,k2)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into ${testTable} select 1,1,1;"
    sql "insert into ${testTable} select 2,2,2;"
    sql "insert into ${testTable} select 3,3,3;"

    test {
        sql "create materialized view k1_k2_k3 as select k1 as a1,k2 as a2,k3 as a3 from ${testTable};"
        exception "errCode = 2,"
    }

    create_sync_mv(db, testTable, "k1_u1", "select k1 as a1 from ${testTable} group by k1;")
    create_sync_mv(db, testTable, "k1_k2_u12", "select k1 as a2,k2 as a3 from ${testTable} group by k1,k2;")
    create_sync_mv(db, testTable, "k1_k2_u21", "select k2 as a4,k1 as a5 from ${testTable} group by k2,k1 order by k2,k1;")
    create_sync_mv(db, testTable, "k1_k2_sumk3", "select k1 as a6,k2 as a7,sum(k3) as a8 from ${testTable} group by k1,k2;")
    sql "insert into ${testTable} select 4,4,4;"

    test {
        sql """
        create table test_rollup (
            `id` int not null,
            `kbool` boolean not null,
            `ktint` tinyint(4) not null,
            `ksint` smallint(6) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `klint` largeint(40) not null
        ) engine=OLAP
        duplicate key(id, kbool, ktint)
        distributed by random buckets auto
        rollup (
            r1 (id, ktint, kbool, ktint, kbint) duplicate key(id)
        )
        properties (
        "replication_num"="1"
        );
        """
        exception "duplicate column name"
    }
}
