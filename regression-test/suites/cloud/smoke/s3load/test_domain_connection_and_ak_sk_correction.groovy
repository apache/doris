suite("test_domain_connection_and_ak_sk_correction") {
    // def clusterMap = loadClusterMap(Config.clusterFile)
    // create table
    def tableName = 'test_domain_connection_and_ak_sk_correction'
    def tableNameOrders = 'test_domain_connection_and_ak_sk_correction_orders'
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """ DROP TABLE IF EXISTS ${tableNameOrders} FORCE"""
    sql """ 
        CREATE TABLE IF NOT EXISTS ${tableName} (
        P_PARTKEY     INTEGER NOT NULL,
        P_NAME        VARCHAR(55) NOT NULL,
        P_MFGR        CHAR(25) NOT NULL,
        P_BRAND       CHAR(10) NOT NULL,
        P_TYPE        VARCHAR(25) NOT NULL,
        P_SIZE        INTEGER NOT NULL,
        P_CONTAINER   CHAR(10) NOT NULL,
        P_RETAILPRICE DECIMAL(15,2) NOT NULL,
        P_COMMENT     VARCHAR(23) NOT NULL 
        )
        DUPLICATE KEY(P_PARTKEY, P_NAME)
        DISTRIBUTED BY HASH(P_PARTKEY) BUCKETS 3;
    """
    sql """
      CREATE TABLE IF NOT EXISTS ${tableNameOrders}  (
        O_ORDERKEY       INTEGER NOT NULL,
        O_CUSTKEY        INTEGER NOT NULL,
        O_ORDERSTATUS    CHAR(1) NOT NULL,
        O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
        O_ORDERDATE      DATE NOT NULL,
        O_ORDERPRIORITY  CHAR(15) NOT NULL,  
        O_CLERK          CHAR(15) NOT NULL, 
        O_SHIPPRIORITY   INTEGER NOT NULL,
        O_COMMENT        VARCHAR(79) NOT NULL
        )
        DUPLICATE KEY(O_ORDERKEY, O_CUSTKEY)
        DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 32;
    """

    result = sql """ select count(*) from ${tableName} """
    logger.info("before load the count is {}", result)

    def label = UUID.randomUUID().toString().replace("-", "")
    logger.info("label is {}", label)
    def result = sql """
        LOAD LABEL ${label}
        (
            DATA INFILE("s3://${getS3BucketName()}/regression/tpch/sf1/part.tbl")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY "|"
            (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp)
        )
        WITH S3
        (
            "AWS_ENDPOINT" = "${getS3Endpoint()}",
            "AWS_ACCESS_KEY" = "${getS3AK()}",
            "AWS_SECRET_KEY" = "${getS3SK()}",
            "AWS_REGION" = "${getS3Region()}"
        );
    """
    logger.info("the first sql result is {}", result)

    result = sql """ select count(*) from ${tableName} """
    logger.info("after load the count is {}", result)

    label = UUID.randomUUID().toString().replace("-", "")
    
    try {
        result = sql """
            LOAD LABEL ${label}
            (
                DATA INFILE("s3://${getS3BucketName()}/regression/tpch/sf1/part.tbl")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY "|"
                (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp)
            )
            WITH S3
            (
                "AWS_ENDPOINT" = "${getS3Endpoint()}1",
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_REGION" = "${getS3Region()}"
            );
        """
        logger.info("the second sql result is {}", result)
        assertTrue(false. "The endpoint is wrong, so the connection test should fale")
    } catch (Exception e) {
        logger.info("the second sql exception result is {}", e.getMessage())
        assertTrue(e.getMessage().contains("Incorrect object storage info"), e.getMessage())
    }

// the following is not implemented, uncomment if implemented
//     label = UUID.randomUUID().toString().replace("-", "")
//     try {
//         result = sql """
//             LOAD LABEL ${label}
//             (
//                 DATA INFILE("s3://${getS3BucketName()}/regression/tpch/sf1/part.tbl")
//                 INTO TABLE ${tableName}
//                 COLUMNS TERMINATED BY "|"
//                 (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp)
//             )
//             WITH S3
//             (
//                 "AWS_ENDPOINT" = "${getS3Endpoint()}",
//                 "AWS_ACCESS_KEY" = "${getS3AK()}1",
//                 "AWS_SECRET_KEY" = "${getS3SK()}",
//                 "AWS_REGION" = "${getS3Region()}"
//             );
//         """
//         logger.info("the third sql result is {}", result)
//         assertTrue(false. "AK is wrong, so the correction of AKSK test should fale")
//     } catch (Exception e) {
//         logger.info("the third sql exception result is {}", e.getMessage())
//         assertTrue(e.getMessage().contains("Incorrect object storage info"), e.getMessage())
//     }
// 
//     label = UUID.randomUUID().toString().replace("-", "")
//     try {
//         result = sql """
//             LOAD LABEL ${label}
//             (
//                 DATA INFILE("s3://${getS3BucketName()}/regression/tpch/sf1/part.tbl")
//                 INTO TABLE ${tableName}
//                 COLUMNS TERMINATED BY "|"
//                 (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp),
//                 DATA INFILE("s3://${getS3BucketName()}1/regression/tpch/sf1/orders.tbl.1", "s3://${getS3BucketName()}/regression/tpch/sf1/orders.tbl.2")
//                 INTO TABLE ${tableNameOrders}
//                 COLUMNS TERMINATED BY "|"
//                 (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, temp)
//             )
//             WITH S3
//             (
//                 "AWS_ENDPOINT" = "${getS3Endpoint()}",
//                 "AWS_ACCESS_KEY" = "${getS3AK()}",
//                 "AWS_SECRET_KEY" = "${getS3SK()}",
//                 "AWS_REGION" = "${getS3Region()}"
//             );
//         """
//         logger.info("the fourth sql result is {}", result)
//         assertTrue(false. "in the second DATA INFILE, the first bucket is wrong, so the sql should fail")
//     } catch (Exception e) {
//         logger.info("the fourth sql exception result is {}", e.getMessage())
//         assertTrue(e.getMessage().contains("Incorrect object storage info"), e.getMessage())
//     }

    result = sql """ select count(*) from ${tableName} """
    logger.info("before load the count is {}", result)

    // test whether using http in endpoint is ok
    label = UUID.randomUUID().toString().replace("-", "")
    logger.info("label is {}", label)
    result = sql """
            LOAD LABEL ${label}
            (
                DATA INFILE("s3://${getS3BucketName()}/regression/tpch/sf1/part.tbl")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY "|"
                (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp)
            )
            WITH S3
            (
                "AWS_ENDPOINT" = "http://${getS3Endpoint()}",
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_REGION" = "${getS3Region()}"
            );
        """
    logger.info("the result of fifth of sql is {}", result)

    result = sql """ select count(*) from ${tableName} """
    logger.info("after load the count is {}", result)

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """ DROP TABLE IF EXISTS ${tableNameOrders} FORCE"""
}
