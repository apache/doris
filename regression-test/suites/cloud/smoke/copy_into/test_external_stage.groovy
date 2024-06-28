suite("smoke_test_external_stage", "smoke") {
    def tableName = "customer_external_stage"
    def externalStageName = "smoke_test_tpch_external"
    def prefix = "tpch/sf0.1"
    try_sql """drop stage if exists ${externalStageName}"""
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
            C_COMMENT     VARCHAR(117) NOT NULL,
            tmp           CHAR(10) NOT NULL
            )
            UNIQUE KEY(C_CUSTKEY)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        """

        sql """
            create stage if not exists ${externalStageName}
            properties ('endpoint' = '${getS3Endpoint()}' ,
            'region' = '${getS3Region()}' ,
            'bucket' = '${getS3BucketName()}' ,
            'prefix' = 'smoke-test' ,
            'ak' = '${getS3AK()}' ,
            'sk' = '${getS3SK()}' ,
            'provider' = '${getS3Provider()}',
            'access_type' = 'aksk',
            'default.file.column_separator' = "|");
        """

        def result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/customer.tbl.gz') properties ('file.type' = 'csv', 'file.compression' = 'gz', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/customer.tbl.gz') properties ('copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        try_sql """drop stage if exists ${externalStageName}"""
    }
}

