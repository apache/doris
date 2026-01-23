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

suite("test_load_columns_from_path", "load_p0") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()
    def tableName = "test_columns_from_path"
    def label = UUID.randomUUID().toString().replace("-", "0")
    def path = "s3://${s3BucketName}/load/product=p1/code=107020/dt=20250202/data.csv"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName} (
            k1 INT,
            k2 INT,
            pd VARCHAR(20) NULL,
            code INT NULL,
            dt DATE
        )
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    // test all three columns with set three
    try {
        sql """
            LOAD LABEL ${label}
            (
                DATA INFILE("${path}")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY ","
                FORMAT AS "CSV"
                (k1, k2)
                COLUMNS FROM PATH AS (product, code, dt)
                SET
                (
                    pd = product,
                    code = code,
                    dt = dt
                )
            )
            WITH S3
            (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            )
            """

        // Wait for load job to finish
        def maxRetry = 60
        def result = ""
        for (int i = 0; i < maxRetry; i++) {
            result = sql_return_maparray "SHOW LOAD WHERE LABEL = '${label}'"
            if (result[0].State == "FINISHED" || result[0].State == "CANCELLED") {
                break
            }
            sleep(1000)
        }

        // Check load job state
        assertEquals("FINISHED", result[0].State)

        // Verify the loaded data
        def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
        assertTrue(rowCount[0][0] > 0, "No data was loaded")

        // Verify columns from path are extracted correctly
        def pathData = sql "SELECT pd, code, dt FROM ${tableName} LIMIT 1"
        assertEquals("p1", pathData[0][0])
        assertEquals(107020, pathData[0][1])
        assertEquals("2025-02-02", pathData[0][2].toString())

    } finally {
        sql """ TRUNCATE TABLE ${tableName} """
    }
    
    // test all three columns with set non-same name column
    label = UUID.randomUUID().toString().replace("-", "1")
    try {
        sql """
            LOAD LABEL ${label}
            (
                DATA INFILE("${path}")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY ","
                FORMAT AS "CSV"
                (k1, k2)
                COLUMNS FROM PATH AS (product, code, dt)
                SET (
                    pd = product
                )
            )
            WITH S3
            (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            )
            """

        // Wait for load job to finish
        def maxRetry = 60
        def result = ""
        for (int i = 0; i < maxRetry; i++) {
            result = sql_return_maparray "SHOW LOAD WHERE LABEL = '${label}'"
            if (result[0].State == "FINISHED" || result[0].State == "CANCELLED") {
                break
            }
            sleep(1000)
        }

        // Check load job state
        assertEquals("FINISHED", result[0].State)

        // Verify the loaded data
        def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
        assertTrue(rowCount[0][0] > 0, "No data was loaded")

        // Verify columns from path are extracted correctly
        def pathData = sql "SELECT pd, code, dt FROM ${tableName} LIMIT 1"
        assertEquals("p1", pathData[0][0])
        assertEquals(107020, pathData[0][1])
        assertEquals("2025-02-02", pathData[0][2].toString())

    } finally {
        sql """ TRUNCATE TABLE ${tableName} """
    }

    // test extracting only one column from path (only product)
    label = UUID.randomUUID().toString().replace("-", "2")
    try {
        sql """
            LOAD LABEL ${label}
            (
                DATA INFILE("${path}")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY ","
                FORMAT AS "CSV"
                (k1, k2)
                COLUMNS FROM PATH AS (product)
                SET
                (
                    pd = product
                )
            )
            WITH S3
            (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            )
            """

        // Wait for load job to finish
        def maxRetry = 60
        def result = ""
        for (int i = 0; i < maxRetry; i++) {
            result = sql_return_maparray "SHOW LOAD WHERE LABEL = '${label}'"
            if (result[0].State == "FINISHED" || result[0]. State == "CANCELLED") {
                break
            }
            sleep(1000)
        }

        // Check load job state
        assertEquals("FINISHED", result[0].State)

        // Verify the loaded data
        def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
        assertTrue(rowCount[0][0] > 0, "No data was loaded")

        // Verify only pd column is extracted from path, code and dt are loaded from CSV file
        def pathData = sql "SELECT pd FROM ${tableName} LIMIT 1"
        assertEquals("p1", pathData[0][0])
        // code and dt should be loaded from CSV file data, not from path
        // The actual values depend on the CSV file content

    } finally {
        sql """ DROP TABLE ${tableName} """
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName} (
            k1 INT,
            k2 INT,
            k3 INT
        )
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    label = UUID.randomUUID().toString().replace("-", "2")
    try {
        sql """
            LOAD LABEL ${label}
            (
                DATA INFILE("s3://${s3BucketName}/load/k1=10/k2=20/test.parquet")
                INTO TABLE ${tableName}
                FORMAT AS "parquet"
                (k1, k2, k3)
            )
            WITH S3
            (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            )
            """

        // Wait for load job to finish
        def maxRetry = 60
        def result = ""
        for (int i = 0; i < maxRetry; i++) {
            result = sql_return_maparray "SHOW LOAD WHERE LABEL = '${label}'"
            if (result[0].State == "FINISHED" || result[0]. State == "CANCELLED") {
                break
            }
            sleep(1000)
        }

        // Check load job state
        assertEquals("FINISHED", result[0].State)

        // Verify the loaded data
        def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
        assertTrue(rowCount[0][0] > 0, "No data was loaded")

        def pathData = sql "SELECT * FROM ${tableName} LIMIT 1"
        logger.info("path data 1: " + pathData)
        // 1 2 3 
        assertEquals(1, pathData[0][0])
        assertEquals(2, pathData[0][1])
        assertEquals(3, pathData[0][2])

    } finally {
        sql """ TRUNCATE TABLE ${tableName} """
    }

    label = UUID.randomUUID().toString().replace("-", "2")


        try {
        sql """
            LOAD LABEL ${label}
            (
                DATA INFILE("s3://${s3BucketName}/load/k1=10/k2=20/test.orc")
                INTO TABLE ${tableName}
                FORMAT AS "orc"
                (k1, k3)
                COLUMNS FROM PATH AS (k2)
            )
            WITH S3
            (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            )
            """

        // Wait for load job to finish
        def maxRetry = 60
        def result = ""
        for (int i = 0; i < maxRetry; i++) {
            result = sql_return_maparray "SHOW LOAD WHERE LABEL = '${label}'"
            if (result[0].State == "FINISHED" || result[0]. State == "CANCELLED") {
                break
            }
            sleep(1000)
        }

        // Check load job state
        assertEquals("FINISHED", result[0].State)

        // Verify the loaded data
        def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
        assertTrue(rowCount[0][0] > 0, "No data was loaded")

        def pathData = sql "SELECT * FROM ${tableName} LIMIT 1"
        logger.info("path data 2: " + pathData)
        // 1 20 3 
        assertEquals(1, pathData[0][0])
        assertEquals(20, pathData[0][1])
        assertEquals(3, pathData[0][2])

    } finally {
        sql """ TRUNCATE TABLE ${tableName} """
    }

    label = UUID.randomUUID().toString().replace("-", "2")
    try {
        sql """
            LOAD LABEL ${label}
            (
                DATA INFILE("s3://${s3BucketName}/load/k1=10/k3=30/test.parquet")
                INTO TABLE ${tableName}
                FORMAT AS "parquet"
                (k2)
                COLUMNS FROM PATH AS (k1,k3)
            )
            WITH S3
            (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            )
            """

        // Wait for load job to finish
        def maxRetry = 60
        def result = ""
        for (int i = 0; i < maxRetry; i++) {
            result = sql_return_maparray "SHOW LOAD WHERE LABEL = '${label}'"
            if (result[0].State == "FINISHED" || result[0]. State == "CANCELLED") {
                break
            }
            sleep(1000)
        }

        // Check load job state
        assertEquals("FINISHED", result[0].State)

        // Verify the loaded data
        def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
        assertTrue(rowCount[0][0] > 0, "No data was loaded")

        def pathData = sql "SELECT * FROM ${tableName} LIMIT 1"
        logger.info("path data 2: " + pathData)
        // 10 2 30 
        assertEquals(10, pathData[0][0])
        assertEquals(2, pathData[0][1])
        assertEquals(30, pathData[0][2])

    } finally {
        sql """ TRUNCATE TABLE ${tableName} """
    }


    label = UUID.randomUUID().toString().replace("-", "2")
    try {
        sql """
            LOAD LABEL ${label}
            (
                DATA INFILE("s3://${s3BucketName}/load/k1=10/k3=30/test.orc")
                INTO TABLE ${tableName}
                FORMAT AS "orc"
                (k1,k2)
                COLUMNS FROM PATH AS (k3)
            )
            WITH S3
            (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            )
            """

        // Wait for load job to finish
        def maxRetry = 60
        def result = ""
        for (int i = 0; i < maxRetry; i++) {
            result = sql_return_maparray "SHOW LOAD WHERE LABEL = '${label}'"
            if (result[0].State == "FINISHED" || result[0]. State == "CANCELLED") {
                break
            }
            sleep(1000)
        }

        // Check load job state
        assertEquals("FINISHED", result[0].State)

        // Verify the loaded data
        def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
        assertTrue(rowCount[0][0] > 0, "No data was loaded")

        def pathData = sql "SELECT * FROM ${tableName} LIMIT 1"
        logger.info("path data 2: " + pathData)
        assertEquals(1, pathData[0][0])
        assertEquals(2, pathData[0][1])
        assertEquals(30, pathData[0][2])


        // [[1, 2, 30]]
    } finally {
        sql """ TRUNCATE TABLE ${tableName} """
    }

}