import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_apsarad_internal_stage_copy_into") {
    // Internal and external stage cross use
    def tableNamExternal = "customer_internal_stage"
    //def token = "greedisgood9999"
    def instanceId = context.config.instanceId
    def cloudUniqueId = context.config.cloudUniqueId
    def token = context.config.metaServiceToken
    def instance_id = context.config.multiClusterInstance

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    for(String values : bes) {
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);

    for (unique_id : beUniqueIdList) {
        resp = get_cluster.call(unique_id);
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    sleep(20000)

    List<List<Object>> showResult  = sql "show clusters"
    assertTrue(showResult.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    sleep(20000)

    showResult  = sql "show clusters"
    assertTrue(showResult.size() == 1);

    for (row : showResult) {
        println row
    }

    try {
        sql """
            CREATE TABLE IF NOT EXISTS ${tableNamExternal} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
            )
            UNIQUE KEY(C_CUSTKEY)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        """
        sql """ DROP TABLE IF EXISTS ${tableNamExternal}; """
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${tableNamExternal}")
    }

    def tableName = "customer_apsaradb_internal_stage"

    def uploadFile = { remoteFilePath, localFilePath ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -u """ + context.config.feCloudHttpUser + ":" + context.config.feCloudHttpPassword)
        strBuilder.append(""" -H fileName:""" + remoteFilePath)
        strBuilder.append(""" -H host:""" + "private")
        strBuilder.append(""" -T """ + localFilePath)
        def feHttpAddress = context.config.isDorisEnv ? context.config.feHttpAddress : context.config.feCloudHttpAddress
        strBuilder.append(""" -L http://""" + feHttpAddress + """/copy/upload""")

        String command = strBuilder.toString()
        logger.info("upload command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
    }

    def createTable = {
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
    }

    def waitInternalStageFilesDeleted = { fileName ->
        def retry = 10
        do {
            Thread.sleep(2000)
            if (checkRecycleInternalStage(token, instanceId, cloudUniqueId, fileName)) {
                Thread.sleep(2000) // wait for copy job kv is deleted
                return
            }
        } while (retry--)
        assertTrue(false, "Internal stage file is not deleted")
    }

    def getCloudConf = {
        configResult = sql """ ADMIN SHOW FRONTEND CONFIG """
        for (def r : configResult) {
            assertTrue(r.size() > 2)
            if (r[0] == "cloud_delete_loaded_internal_stage_files") {
                return (r[1] == "true")
            }
        }
        return false
    }

    boolean cloud_delete_loaded_internal_stage_files = getCloudConf()
    logger.info("cloud_delete_loaded_internal_stage_files=" + cloud_delete_loaded_internal_stage_files)

    try {
        setFeConfig('apsaradb_env_enabled', true)
        def fileName = "internal_customer.csv"
        def filePath = "${context.config.dataPath}/cloud/copy_into/" + fileName
        def remoteFileName = fileName + "test_apsaradb_internal_stage"
        uploadFile(remoteFileName, filePath)

        createTable()
        def result = sql " copy into ${tableName} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        result = sql " copy into ${tableName} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        if (cloud_delete_loaded_internal_stage_files) {
            // check file is deleted
            waitInternalStageFilesDeleted(remoteFileName)
            // check copy job and file keys are deleted
            uploadFile(remoteFileName, filePath)
            result = sql " copy into ${tableName} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
            logger.info("copy result: " + result)
            assertTrue(result.size() == 1)
            assertTrue(result[0].size() == 8)
            assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
            // check file is deleted
            waitInternalStageFilesDeleted(remoteFileName)
        }

    } finally {
        setFeConfig('apsaradb_env_enabled', false)
        sql """ DROP TABLE IF EXISTS ${tableName}; """
    }
}
