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
import org.awaitility.Awaitility

/*
* make sure compactions are triggered and sleep a short time ( maybe 10 seconds ) waiting for compactions starting.
*/
Suite.metaClass.checkComactionStatus = { String backendIP, String backendPort, String tabletID ->
    def (code, out, err) = be_get_compaction_status(backendIP, backendPort, tabletID)
    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
    if (code != 0) {
        return false
    }

    def compactionStatus = parseJson(out.trim())
    if ("success" == compactionStatus.status.toLowerCase()) {
        // run_status == true means compactions are running
        // run_status == false means compaction are finished
        return !compactionStatus.run_status
    } else {
        return false
    }
}

Suite.metaClass.assertCompactionStatus = { String backendIP, String backendPort, String tabletID ->
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, SECONDS).untilAsserted({
        assert checkComactionStatus(backendIP, backendPort, tabletID)
    })
}

Suite.metaClass.assertCompactionStatusAtMost = { String backendIP, String backendPort, String tabletID, long t, TimeUnit tu ->
    Awaitility.await().atMost(t, tu).pollInterval(1, SECONDS).untilAsserted({
        assert checkComactionStatus(backendIP, backendPort, tabletID)
    })
}

// let table do full compaction, and waiting for them done.
// TODO: auto compaction what.
Suite.metaClass.doCompactionWaitDone = { String tableName ->
    // get table tablets.
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    // trigger compactions for all tablets in ${tableName}
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        def backend_id = tablet.BackendId
        def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        if (compactJson.status.toLowerCase() == "fail") {
            assertEquals(disableAutoCompaction, false)
            logger.info("Compaction was done automatically!")
        }
        if (disableAutoCompaction) {
            assertEquals("success", compactJson.status.toLowerCase())
        }
    }

    // waiting compaction to start
    Thread.sleep(10000)

    // wait for all compactions done
    for (def tablet in tablets) {
        assertCompactionStatus(backendId_to_backendIP.get(tablet.BackendId), backendId_to_backendHttpPort.get(tablet.BackendId), tablet.TabletId)
    }
}

