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

suite("test_recycler_with_internal_copy") {
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId

    def tableName = "test_recycler_with_internal_copy"
    def fileName = "test_recycler_with_internal_copy.csv"
    def filePath = "${context.config.dataPath}/cloud/copy_into/internal_customer.csv"

    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("""curl -u """ + context.config.feCloudHttpUser + ":" + context.config.feCloudHttpPassword)
    strBuilder.append(""" -H fileName:""" + fileName)
    if (getS3Provider().equalsIgnoreCase("azure")) {
        strBuilder.append(""" -H x-ms-blob-type:BlockBlob """)
    }
    strBuilder.append(""" -T """ + filePath)
    strBuilder.append(""" -L http://""" + context.config.feCloudHttpAddress + """/copy/upload""")

    String command = strBuilder.toString()
    logger.info("upload command=" + command)
    def process = command.toString().execute()
    def code = process.waitFor()
    def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    def out = process.getText()
    logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
        C_CUSTKEY     INTEGER NOT NULL,
        C_NAME        VARCHAR(25) NOT NULL,
        C_ADDRESS     VARCHAR(40) NOT NULL,
        C_NATIONKEY   INTEGER NOT NULL,
        C_PHONE       CHAR(15) NOT NULL,
        C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
        C_MKTSEGMENT  CHAR(10) NOT NULL,
        C_COMMENT     VARCHAR(117) NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
    """

    def result = sql " copy into ${tableName} from @~('${fileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
    qt_sql " SELECT COUNT(*) FROM ${tableName}; "

    result = sql " copy into ${tableName} from @~('${fileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
    qt_sql " SELECT COUNT(*) FROM ${tableName}; "


    int retry = 15
    boolean success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000) // 20s
        if (checkRecycleInternalStage(token, instanceId, cloudUniqueId, fileName)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)

    logger.info("upload command=" + command)
    process = command.toString().execute()
    code = process.waitFor()
    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    out = process.getText()
    logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)

    result = sql " copy into ${tableName} from @~('${fileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
    qt_sql " SELECT COUNT(*) FROM ${tableName}; "

    String[][] tabletInfoList = sql """ show tablets from ${tableName}; """
    logger.debug("tabletInfoList:${tabletInfoList}")

    HashSet<String> tabletIdSet= new HashSet<String>()
    for (tabletInfo : tabletInfoList) {
        tabletIdSet.add(tabletInfo[0])
    }
    logger.info("tabletIdSet:${tabletIdSet}")

    // drop table
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    retry = 15
    success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000) // 20s
        if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, tabletIdSet)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)
}
