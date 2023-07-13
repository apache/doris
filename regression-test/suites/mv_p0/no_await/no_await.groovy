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

    def waitDrop = {
        def try_times = 100
        def result = "null"
        while (!result.contains("FINISHED")) {
            result = (sql "SHOW ALTER TABLE MATERIALIZED VIEW ORDER BY CreateTime DESC LIMIT 1;")[0]
            Thread.sleep(500)
            try_times -= 1
            assertTrue(try_times > 0)
        }

        sql "drop materialized view k12s3m on agg_have_dup_base;"
        while (!(sql "show create materialized view k12s3m on agg_have_dup_base;").empty) {
            sleep(100)
            try_times -= 1
            assertTrue(try_times > 0)
        }
    }

    sql 'drop table if exists agg_have_dup_base'
    sql '''
        create table agg_have_dup_base (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1, k2, k3)
        distributed by hash(k1) buckets 3
        properties("replication_num" = "1");
    '''
    sql "insert into agg_have_dup_base select e1, -4, -4, 'd' from (select 1 k1) as t lateral view explode_numbers(10000) tmp1 as e1;"
    // do not await
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'

    waitDrop()
    sql "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;"
    sql 'insert into agg_have_dup_base select -4, -4, -4, \'d\''
    qt_mv 'select sum(k1) from agg_have_dup_base'
}
