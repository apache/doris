import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_internal_stage_smoke", "smoke") {
    def tableName = "customer_internal_stage"
    def fileName = "internal_customer.csv"
    def filePath = "${context.config.dataPath}/cloud/smoke/copy_into/" + fileName
    def remoteFileName = fileName + "smoke_test_internal_stage"

    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("""curl -vv -u """ + context.config.feCloudHttpUser + ":" + context.config.feCloudHttpPassword)
    strBuilder.append(""" -H fileName:""" + remoteFileName)
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

    try {
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
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}

