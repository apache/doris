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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

suite('test_clone_no_missing_version', 'docker') {
    def tbl = 'tbl_test_clone_no_missing_version'
    def options = new ClusterOptions()
    options.feConfigs += [
        'disable_tablet_scheduler=true',
        'tablet_checker_interval_ms=500',
        'schedule_batch_size=1000',
        'schedule_slot_num_per_hdd_path=1000',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=100000', // don't report tablets
    ]

    options.feNum = 3
    options.cloudMode = false
    options.connectToFollower = true

    docker(options) {
        sql '''SET forward_to_master = false'''

        sql """
            CREATE TABLE ${tbl} (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1;
        """

        sql """INSERT INTO ${tbl} VALUES (1) """
        sql """INSERT INTO ${tbl} VALUES (2) """

        def tablets = sql_return_maparray "SHOW TABLETS FROM ${tbl}"
        assertEquals(3, tablets.size())

        def originTablet = tablets[0]

        sql """ ADMIN SET REPLICA VERSION PROPERTIES (
            "tablet_id" = "${originTablet.TabletId}", "backend_id" = "${originTablet.BackendId}",
            "version" = "3", "last_failed_version" = "4"
            ); """

        def checkTabletIsGood = { good ->
            def changedTablet = sql_return_maparray("SHOW TABLETS FROM ${tbl}").find { it.BackendId == originTablet.BackendId }
            assertNotNull(changedTablet)
            assertEquals(3L, changedTablet.Version.toLong())
            if (good) {
                assertEquals(-1L, changedTablet.LstFailedVersion.toLong())
            } else {
                assertEquals(4L, changedTablet.LstFailedVersion.toLong())
            }
        }

        sleep 1000

        checkTabletIsGood(false)

        sql "ADMIN SET FRONTEND CONFIG ('disable_tablet_scheduler' = 'false')"

        sleep 5000
        checkTabletIsGood(true)
    }
}
