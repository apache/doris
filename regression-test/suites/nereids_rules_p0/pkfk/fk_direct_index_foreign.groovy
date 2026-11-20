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

import java.util.concurrent.TimeUnit

import org.awaitility.Awaitility

suite("fk_direct_index_foreign") {
    def parentTable = "fk_direct_index_parent"
    def childTable = "fk_direct_index_child"
    def rollupName = "fk_direct_index_rollup"

    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"

    sql "drop table if exists ${childTable}"
    sql "drop table if exists ${parentTable}"
    sql """
        create table ${parentTable} (
            id int not null
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        create table ${childTable} (
            g1 int not null,
            g2 int not null,
            parent_id int sum not null
        ) aggregate key(g1, g2)
        distributed by hash(g1) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        alter table ${parentTable} add constraint fk_direct_index_pk primary key(id)
    """
    sql """
        alter table ${childTable} add constraint fk_direct_index_fk
        foreign key(parent_id) references ${parentTable}(id)
    """

    sql "insert into ${parentTable} values (1)"
    sql "insert into ${childTable} values (10, 1, 1), (10, 2, 1)"
    sql "sync"
    sql "alter table ${childTable} add rollup ${rollupName}(g1, parent_id)"

    /*
     * Wait for the rollup job that the preceding ALTER submitted. A cancelled job is surfaced
     * immediately as an assertion failure instead of allowing the later direct-index query to fail
     * with a less useful missing-index error.
     */
    def getRollupState = {
        def jobs = sql """
            show alter table rollup where TableName='${childTable}'
            order by CreateTime desc limit 1
        """
        return jobs.isEmpty() ? "PENDING" : jobs[0][8]
    }
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
        def state = getRollupState()
        if (state == "CANCELLED") {
            assertEquals("FINISHED", state)
        }
        return state == "FINISHED"
    })

    // Dropping g2 from the rollup merges the two base rows and changes parent_id from 1 to 2.
    order_qt_direct_index_rows """
        select g1, parent_id
        from ${childTable} index ${rollupName}
        order by g1, parent_id
    """

    /*
     * The direct index row no longer satisfies the declared FK. Its join to parent id 1 must remain;
     * otherwise FK elimination returns one row even though the real inner join returns none.
     */
    explain {
        sql """
            shape plan
            select f.parent_id
            from ${parentTable} p
            inner join ${childTable} index ${rollupName} f on p.id = f.parent_id
        """
        contains "INNER_JOIN"
    }
    qt_direct_index_join_count """
        select count(*)
        from ${parentTable} p
        inner join ${childTable} index ${rollupName} f on p.id = f.parent_id
    """
}
