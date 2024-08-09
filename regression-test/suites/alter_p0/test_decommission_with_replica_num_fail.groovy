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

suite('test_decommission_with_replica_num_fail', 'nonConcurrent') {
    if (isCloudMode()) {
        return
    }

    def tbl = 'test_decommission_with_replica_num_fail'
    def backends = sql_return_maparray('show backends')
    def replicaNum = 0
    def targetBackend = null
    for (def be : backends) {
        def alive = be.Alive.toBoolean()
        def decommissioned = be.SystemDecommissioned.toBoolean()
        if (alive && !decommissioned) {
            replicaNum++
            targetBackend = be
        }
    }
    assertTrue(replicaNum > 0)

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql """
        CREATE TABLE ${tbl}
        (
            k1 int,
            k2 int
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 6
        PROPERTIES
        (
            "replication_num" = "${replicaNum}"
        );
    """

    // fix set force_olap_table_replication_num
    sql "ALTER TABLE ${tbl} SET ('default.replication_num' = '${replicaNum}')"
    sql "ALTER TABLE ${tbl} MODIFY PARTITION (*) SET ('replication_num' = '${replicaNum}')"

    try {
        test {
            sql "ALTER SYSTEM DECOMMISSION BACKEND '${targetBackend.Host}:${targetBackend.HeartbeatPort}'"
            exception "otherwise need to decrease the partition's replication num"
        }
    } finally {
        sql "CANCEL DECOMMISSION BACKEND '${targetBackend.Host}:${targetBackend.HeartbeatPort}'"
        backends = sql_return_maparray('show backends')
        for (def be : backends) {
            logger.info("backend=${be}")
        }
    }
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
}
