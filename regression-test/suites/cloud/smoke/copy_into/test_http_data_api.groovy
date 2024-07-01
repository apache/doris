import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_internal_stage_http_data_api", "smoke") {
    def tableName = "customer_internal_stage_http_data_api"
    def fileName = "internal_customer.csv"
    def filePath = "${context.config.dataPath}/cloud/smoke/copy_into/" + fileName
    def remoteFileName = fileName + "test_http_data_api"

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
    logger.info("Http data api Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)

    // if aliyun, test internal endpoint
    if ("oss".equals(getS3Provider())) {
        def tmp = new StringBuilder(strBuilder)
        def tmp1 = new StringBuilder(strBuilder)
        // test internal
        strBuilder.append(""" -H Host:www.selectdb.com --connect-timeout 5 -vv""")
        command = strBuilder.toString()
        logger.info("Test aliyun internal endpoint command=" + command)
        process = command.toString().execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        out = process.getText()
        def location = null
        err.eachLine {if(it.startsWith("< Location")) {
            location = it
        }}
        if (null == location) {
            assertTrue(false)
        }
        logger.info("location: " + location)
        // < Location: http://selectdb-online-cn-shenzhen-bucket1667494333.oss-cn-shenzhen-internal.aliyuncs.com/m-xoad88n48839bauewv/stage/admin/admin/internal_customer.csv?Expires=1676956170&OSSAccessKeyId=LTAI5tCRA18qGLf6CiD9DFRv&Signature=GVFAAhSyNnZzFpXaakoc8gNietg%3D
        // oss-cn-shenzhen-internal, assert contain '-internal'
        assertTrue(location.toString().contains("-internal"))

        // test non internal, host is ip, so non internal
        tmp.append(""" -H Host:210.211.212.213:8888 --connect-timeout 5 -vv""")
        command = tmp.toString()
        logger.info("Test aliyun non internal endpoint command=" + command)
        process = command.toString().execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        out = process.getText()
        location = null
        err.eachLine {if(it.startsWith("< Location")) {
            location = it
        }}
        if (null == location) {
            assertTrue(false)
        }
        // curl -u admin:smoke_Test -H fileName:internal_customer.csv -T /mnt/disk3/smoke-test/bin/regression-test/data/cloud/smoke/copy_into/internal_customer.csv -L http://119.23.41.172:39590/copy/upload -H Host:210.211.212.213:8888 --connect-timeout 5 -vv
        assertFalse(location.toString().contains("-internal"))

        // test non internal, host is special contail 'selectdb.cloud', so non internal
        tmp1.append(""" -H Host:xxxx.studio.selectdb.cloud --connect-timeout 5 -vv""")
        command = tmp1.toString()
        logger.info("Test aliyun non internal endpoint command=" + command)
        process = command.toString().execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        out = process.getText()
        location = null
        err.eachLine {if(it.startsWith("< Location")) {
            location = it
        }}
        if (null == location) {
            assertTrue(false)
        }
    }

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
