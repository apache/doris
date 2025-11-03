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

import org.apache.doris.regression.suite.Suite
import org.codehaus.groovy.runtime.IOGroovyMethods

Suite.metaClass.doCloudCompaction = { String tableName /* param */ ->
    // which suite invoke current function?
    Suite suite = delegate as Suite
    //function body
    suite.getLogger().info("Test plugin: suiteName: ${suite.name}, tableName:${tableName}".toString())

    //BackendId,Cluster,IP,HeartbeatPort,BePort,HttpPort,BrpcPort,LastStartTime,LastHeartbeat,Alive,SystemDecommissioned,ClusterDecommissioned,TabletNum,DataUsedCapacity,AvailCapacity,TotalCapacity,UsedPct,MaxDiskUsedPct,RemoteUsedCapacity,Tag,ErrMsg,Version,Status
    String[][] backends = sql """ show backends; """
    suite.getLogger().info("backends: ${backends}".toString())

    assertTrue(backends.size() > 0)
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def clusterToBackendId = [:]

    for (String[] backend in backends) {
        backendIdToBackendIP.put(backend[0], backend[2])
        backendIdToBackendHttpPort.put(backend[0], backend[5])
        def tagJson = parseJson(backend[19])
        if (!clusterToBackendId.containsKey(tagJson.cloud_cluster_name)) {
            clusterToBackendId.put(tagJson.cloud_cluster_name, backend[0])
        }
    }
    suite.getLogger().info("backendIdToBackendIP: ${backendIdToBackendIP}".toString())
    suite.getLogger().info("backendIdToBackendHttpPort: ${backendIdToBackendHttpPort}".toString())
    suite.getLogger().info("clusterToBackendId: ${clusterToBackendId}".toString())

    def cluster0 = clusterToBackendId.keySet()[0]
    def backend_id0 = clusterToBackendId.get(cluster0)

    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
    String[][] tablets = sql """ show tablets from ${tableName}; """
    def doCompaction = { backend_id, compact_type ->
        // trigger compactions for all tablets in ${tableName}
        for (String[] tablet in tablets) {
            Thread.sleep(10)
            String tablet_id = tablet[0]
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://")
            sb.append(backendIdToBackendIP.get(backend_id))
            sb.append(":")
            sb.append(backendIdToBackendHttpPort.get(backend_id))
            sb.append("/api/compaction/run?tablet_id=")
            sb.append(tablet_id)
            sb.append("&compact_type=${compact_type}")

            String command = sb.toString()
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            //assertEquals("success", compactJson.status.toLowerCase())
        }

        // wait for all compactions done
        for (String[] tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet[0]
                StringBuilder sb = new StringBuilder();
                sb.append("curl -X GET http://")
                sb.append(backendIdToBackendIP.get(backend_id))
                sb.append(":")
                sb.append(backendIdToBackendHttpPort.get(backend_id))
                sb.append("/api/compaction/run_status?tablet_id=")
                sb.append(tablet_id)

                String command = sb.toString()
                logger.info(command)
                process = command.execute()
                code = process.waitFor()
                err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                out = process.getText()
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                //assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }
    }
    doCompaction.call(backend_id0, "cumulative")
    //doCompaction.call(backend_id0, "base")
    return
}
logger.info("Added 'doCloudCompaction' function to Suite")


