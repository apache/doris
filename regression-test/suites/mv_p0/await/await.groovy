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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("await") {

    String db = context.config.getDbNameByFile(context.file)

    def tblName = "agg_have_dup_base_await"
    def waitDrop = {
        def try_times = 1000
        def result = "null"
        sql "sync;"
        while (!result.contains("FINISHED")) {
            result = (sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tblName}' ORDER BY CreateTime DESC LIMIT 1;")[0]
            if (!result.contains("RUNNING")&&!result.contains("PENDING")&&!result.contains("FINISHED")&&!result.contains("WAITING_TXN")) {
                assertTrue(false)
            }
            log.info("result: ${result}")
            Thread.sleep(3000)
            try_times -= 1
            assertTrue(try_times > 0)
        }
        sql "sync;"
        sql "drop materialized view k12s3m on ${tblName};"
        while (!(sql "show create materialized view k12s3m on ${tblName};").empty) {
            sleep(100)
            try_times -= 1
            assertTrue(try_times > 0)
        }
        sql "sync;"
    }

    // this mv rewrite would not be rewritten in RBO phase, so set TRY_IN_RBO explicitly to make case stable
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"

    sql "drop table if exists ${tblName} force;"
    sql """
        create table ${tblName} (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1, k2, k3)
        distributed by hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    sql "insert into ${tblName} select e1, -4, -4, 'd' from (select 1 k1) as t lateral view explode_numbers(10000) tmp1 as e1;"
    // do not await
    create_sync_mv(db, tblName, "k12s3m", """select k1 as a1,sum(k2) as a2,max(k2) as a3 from ${tblName} group by k1;""")

    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as a4,sum(k2) as a5,max(k2) as a6 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as b1,sum(k2) as b2,max(k2) as b3 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as b4,sum(k2) as b5,max(k2) as b6 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as x1,sum(k2) as x2,max(k2) as x3 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as x4,sum(k2) as x5,max(k2) as x6 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as c1,sum(k2) as c2,max(k2) as c3 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as c4,sum(k2) as c5,max(k2) as c6 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as d1,sum(k2) as d2,max(k2) as d3 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as d4,sum(k2) as d5 ,max(k2) as d6 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as e1,sum(k2) as e2,max(k2) as e3 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as e4,sum(k2) as e5,max(k2) as e6 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as f1,sum(k2) as f2,max(k2) as f3 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as f4,sum(k2) as f5,max(k2) as f6 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as s1,sum(k2) as s2,max(k2) as s3 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as s4,sum(k2) as s5,max(k2) as s6 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as w1,sum(k2) as w2,max(k2) as w3 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    create_sync_mv(db, tblName, "k12s3m", """select k1 as w4,sum(k2) as w5,max(k2) as w6 from ${tblName} group by k1;""")
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    sql "sync;"
    qt_mv "select sum(k1) from ${tblName}"
}
