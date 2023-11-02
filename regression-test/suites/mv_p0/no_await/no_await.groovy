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

suite ("no_await") {

    def tblName = "agg_have_dup_base_no_await"
    def waitDrop = {
        def try_times = 1000
        def result = "null"
        sql "sync;"
        while (!result.contains("FINISHED")) {
            result = (sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tblName}' ORDER BY CreateTime DESC LIMIT 1;")[0]
            if (result.contains("CANCELLED")) {
                log.info("result: ${result}")
                assertTrue(false)
            }
            Thread.sleep(1100)
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
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from ${tblName} group by k1;"
    sql "insert into ${tblName} select -4, -4, -4, \'d\'"
    qt_mv "select sum(k1) from ${tblName}"
}
