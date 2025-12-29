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

suite("test_recycler_clone_instance") {
    def enableMultiVersionStatus = context.config.enableMultiVersionStatus
    def enableClusterSnapshot = context.config.enableClusterSnapshot
    def metaServiceHttpAddress = context.config.metaServiceHttpAddress
    def dbName = "regression_test_cloud_p0_recycler"

    if (!enableClusterSnapshot || !enableMultiVersionStatus) {
        logger.info("enableClusterSnapshot or enableMultiVersionStatus is not true, skip clone instance")
        return
    }

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    def instanceId = System.getenv("DORIS_MS_INSTANCE_ID")
    logger.info("DORIS_MS_INSTANCE_ID: ${instanceId}")

    assert instanceId != null, "DORIS_MS_INSTANCE_ID is not set"

    def username = "root"

    logger.info("Querying cluster snapshots")
    def snapshotResult = sql """ SELECT * FROM information_schema.cluster_snapshots """
    logger.info("Snapshot result: ${snapshotResult}")

    assert snapshotResult != null && !snapshotResult.isEmpty(), "No snapshots found, cannot run this test"
    
    def latestSnapshot = snapshotResult[-1]
    def snapshotId = latestSnapshot[0]
    def snapshotLabel = latestSnapshot[9]
    logger.info("Latest snapshot_id: ${snapshotId}, snapshot_label: ${snapshotLabel}")
    
    logger.info("Getting FE information")
    def feResult = sql """ SHOW FRONTENDS """
    logger.info("FE result: ${feResult}")
    
    def masterFe = null
    for (def fe : feResult) {
        if (fe[8] == "true") {
            masterFe = fe
            break
        }
    }

    assert masterFe != null, "No master FE found"
    
    def masterFeHost = masterFe[1]
    logger.info("Master FE host: ${masterFeHost}")
    
    def dorisHome = System.getenv("DORIS_INSTALL_PATH")
    logger.info("DORIS_INSTALL_PATH: ${dorisHome}")

    assert dorisHome != null, "DORIS_INSTALL_PATH is not set"

    def executeCommand = { String cmd, Boolean mustSuc ->
        try {
            logger.info("Execute: ${cmd}")
            def proc = new ProcessBuilder("/bin/bash", "-c", cmd).redirectErrorStream(true).start()
            int exitcode = proc.waitFor()
            def output = proc.text
            if (exitcode != 0) {
                logger.info("Exit code: ${exitcode}, output: ${output}")
                if (mustSuc == true) {
                    throw new Exception("Command failed with exit code ${exitcode}: ${output}")
                }
            }
            return output
        } catch (IOException e) {
            logger.error("Execute timeout: ${e.message}")
            throw new Exception("Execute timeout: ${e.message}")
        }
    }

    logger.info("Getting BE information and stopping all BEs and FEs")
    def beResult = sql """ SHOW BACKENDS """
    logger.info("BE result: ${beResult}")
    
    def beHosts = []
    for (def be : beResult) {
        def beHost = be[1]
        beHosts.add(beHost)
    }
    
    logger.info("BE hosts: ${beHosts}")

    logger.info("Cleaning storage directory on all BEs")
    def storagePath = "${dorisHome}/be/storage/*"
    deleteRemotePathOnAllBE(username, storagePath)
    logger.info("Cleaned storage directory on all BEs")

    logger.info("Stopping master FE on ${masterFeHost} and cleaning doris-meta directory")
    def stopFeCmd = "ssh -o StrictHostKeyChecking=no ${username}@${masterFeHost} \"${dorisHome}/fe/bin/stop_fe.sh\""
    executeCommand(stopFeCmd, true)
    logger.info("Master FE stopped successfully")

    def cleanMetaCmd = "ssh -o StrictHostKeyChecking=no ${username}@${masterFeHost} \"rm -rf ${dorisHome}/fe/doris-meta/*\""
    executeCommand(cleanMetaCmd, true)
    logger.info("Cleaned doris-meta directory")

    for (def beHost : beHosts) {
        logger.info("Stopping BE on ${beHost}")
        def stopBeCmd = "ssh -o StrictHostKeyChecking=no ${username}@${beHost} \"${dorisHome}/be/bin/stop_be.sh\""
        executeCommand(stopBeCmd, true)
        logger.info("BE on ${beHost} stopped successfully")
    }
    
    logger.info("Creating snapshot_info.json")
    
    def newInstanceId = instanceId + "_cloned"
    def instanceName = "cloned_instance"
    
    def snapshotInfoJson = """{"from_instance_id":"${instanceId}","from_snapshot_id":"${snapshotId}","instance_id":"${newInstanceId}","name":"${instanceName}","is_successor":true}"""
    
    logger.info("Snapshot info JSON content: ${snapshotInfoJson}")
    
    def jsonBase64 = snapshotInfoJson.bytes.encodeBase64().toString()
    
    def createJsonCmd = "ssh -o StrictHostKeyChecking=no ${username}@${masterFeHost} \"echo '${jsonBase64}' | base64 -d > ${dorisHome}/fe/snapshot_info.json\""
    executeCommand(createJsonCmd, true)
    logger.info("Created snapshot_info.json successfully")
    
    def verifyJsonCmd = "ssh -o StrictHostKeyChecking=no ${username}@${masterFeHost} \"cat ${dorisHome}/fe/snapshot_info.json\""
    def jsonContent = executeCommand(verifyJsonCmd, true)
    logger.info("Verified snapshot_info.json content: ${jsonContent}")

    logger.info("Starting FE with snapshot_info.json")
    def startFeCmd = "ssh -o StrictHostKeyChecking=no ${username}@${masterFeHost} \"cd ${dorisHome}/fe/bin && export JAVA_HOME=${DORIS_JAVA_HOME} && ./start_fe.sh --daemon --cluster_snapshot snapshot_info.json\""
    executeCommand(startFeCmd, true)
    logger.info("Master FE started successfully with cluster snapshot")
    
    logger.info("Waiting for FE to be ready...")
    Thread.sleep(60000)

    context.reconnectFe()
    
    logger.info("Starting all BEs")
    for (def beHost : beHosts) {
        logger.info("Starting BE on ${beHost}")
        def startBeCmd = "ssh -o StrictHostKeyChecking=no ${username}@${beHost} \"cd ${dorisHome}/be/bin && export JAVA_HOME=${DORIS_JAVA_HOME} && ./start_be.sh --daemon\""
        executeCommand(startBeCmd, true)
        logger.info("BE on ${beHost} started successfully")
    }
    
    logger.info("Waiting for BEs to be ready...")
    Thread.sleep(60000)
    
    logger.info("All steps completed successfully. FE and BEs have been restarted with cloned instance.")
}
