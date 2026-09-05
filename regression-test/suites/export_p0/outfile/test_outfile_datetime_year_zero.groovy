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

// DATETIME starts at 0000-01-01 00:00:00, but the Parquet reader used to reject every timestamp
// below 0001-01-01, so a value Doris accepts, stores and exports could not be read back out of
// Doris's own file. The gate was also applied to the raw instant *before* the timezone offset,
// which lost representable values near both ends of the range whenever the session timezone was
// not UTC. DATETIME is a wall-clock type, so its rendering does not depend on the session
// timezone: the same expected strings must come back under every timezone and both scanners.
// See https://github.com/apache/doris/issues/67447
suite("test_outfile_datetime_year_zero", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName")

    def outFilePath = "${bucket}/outfile/datetime_year_zero/exp_"

    sql """ DROP TABLE IF EXISTS test_outfile_datetime_year_zero_table """
    sql """
        CREATE TABLE test_outfile_datetime_year_zero_table (
            `id` INT NOT NULL,
            `ts` DATETIME(6) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    // Row 1 and row 10 are the two ends of the DATETIME domain: east of UTC row 1's instant falls
    // below the civil minimum and west of UTC row 10's rises above the maximum, so they are the
    // rows that pin "apply the offset, then judge the range". Rows 3 and 4 sit inside
    // 0000-01-01 .. 0000-02-28, the window where Doris's day numbering and the proleptic
    // Gregorian ordinal disagree, and rows 2, 5, 7 and 9 are the ones from the report.
    sql """
        INSERT INTO test_outfile_datetime_year_zero_table VALUES
            (1,  '0000-01-01 00:00:00'),        (2,  '0000-01-01 12:34:56'),
            (3,  '0000-01-02 00:00:00'),        (4,  '0000-02-28 23:59:59.999999'),
            (5,  '0000-03-01 00:00:00'),        (6,  '0001-01-01 00:00:00'),
            (7,  '1969-12-31 23:59:59'),        (8,  '1970-01-01 00:00:00'),
            (9,  '2024-01-01 12:00:00'),        (10, '9999-12-31 23:59:59.999999'),
            (11, NULL);
    """

    def expected = [['1', '0000-01-01 00:00:00.000000'], ['2', '0000-01-01 12:34:56.000000'],
                    ['3', '0000-01-02 00:00:00.000000'], ['4', '0000-02-28 23:59:59.999999'],
                    ['5', '0000-03-01 00:00:00.000000'], ['6', '0001-01-01 00:00:00.000000'],
                    ['7', '1969-12-31 23:59:59.000000'], ['8', '1970-01-01 00:00:00.000000'],
                    ['9', '2024-01-01 12:00:00.000000'], ['10', '9999-12-31 23:59:59.999999'],
                    ['11', null]]

    def readSource = { ->
        return sql("""SELECT CAST(id AS STRING), CAST(ts AS STRING)
                      FROM test_outfile_datetime_year_zero_table ORDER BY id""")
    }
    assertEquals(expected, readSource(), "the table itself does not hold the values under test")

    def outfile_to_S3 = { format ->
        def res = sql """
            SELECT * FROM test_outfile_datetime_year_zero_table t
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        return res[0][3]
    }

    def uriOf = { outfile_url, format ->
        return "http://${bucket}.${s3_endpoint}" +
                outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1) + "0." + format
    }

    def s3Table = { uri, format, extraProps = "" ->
        return """ S3 (
            "uri" = "${uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "format" = "${format}",
            "region" = "${region}"${extraProps}
        ) """
    }
    def timestampTzProp = ',\n            "enable_mapping_timestamp_tz" = "true"'

    def originalTimeZone = sql("SHOW VARIABLES LIKE 'time_zone'")[0][1]
    def originalScannerV2 = sql("SHOW VARIABLES LIKE 'enable_file_scanner_v2'")[0][1]

    try {
        // The Parquet writer encodes DATETIME as an instant in the session timezone, so the
        // timezone decides how far each end of the range sits from the raw bound the reader used
        // to check. UTC alone would not have caught the offset half of the defect.
        for (String timeZone : ["+00:00", "+08:00", "-05:00"]) {
            sql """ set time_zone = '${timeZone}' """
            assertEquals(expected, readSource(),
                         "DATETIME rendering must not depend on the session timezone")

            def uri = uriOf(outfile_to_S3("parquet"), "parquet")
            // Only FileScannerV2, which is the default. The legacy scanner is deliberately not
            // asserted against: it truncates a negative epoch value towards zero instead of
            // flooring it, so any pre-1970 timestamp with a sub-second part moves forward by one
            // second there -- 0000-02-28 23:59:59.999999 lands on the proleptic-only 0000-02-29
            // and comes back NULL. That is a separate defect in a path this fix does not touch.
            sql """ set enable_file_scanner_v2 = true """
            def readBack = sql """
                SELECT CAST(id AS STRING), CAST(ts AS STRING) FROM ${s3Table(uri, "parquet")}
                ORDER BY id;
            """
            assertEquals(expected, readBack,
                         "parquet round trip changed the values (time_zone=${timeZone})")

            // A conversion failure is reported as NULL or as the invalid 0000-00-00 sentinel
            // rather than as an error, so a scan that never materializes the column still counts
            // every row. Comparing the two counts is what makes silent value loss visible.
            def counts = sql """
                SELECT CAST(COUNT(*) AS STRING), CAST(COUNT(ts) AS STRING),
                       CAST(COUNT(CASE WHEN ts < '0001-01-01' THEN 1 END) AS STRING)
                FROM ${s3Table(uri, "parquet")};
            """
            assertEquals([['11', '10', '5']], counts,
                         "year-zero rows are missing from the file scan (time_zone=${timeZone})")

            // Reading a UTC-adjusted Parquet timestamp as TIMESTAMPTZ takes a different failure
            // path from DATETIMEV2, and that is the one the report saw turn into NULL.
            def tzTable = s3Table(uri, "parquet", timestampTzProp)
            def tzCounts = sql """
                SELECT CAST(COUNT(*) AS STRING), CAST(COUNT(ts) AS STRING) FROM ${tzTable};
            """
            assertEquals([['11', '10']], tzCounts,
                         "year-zero rows became NULL as TIMESTAMPTZ (time_zone=${timeZone})")
        }

        sql """ set time_zone = '${originalTimeZone}' """

        // ORC materializes the same values through its own reader, which never had the year-one
        // floor. It is the control: Parquet has to agree with it.
        def orcUri = uriOf(outfile_to_S3("orc"), "orc")
        def orcReadBack = sql """
            SELECT CAST(id AS STRING), CAST(ts AS STRING) FROM ${s3Table(orcUri, "orc")} ORDER BY id;
        """
        assertEquals(expected, orcReadBack, "orc round trip changed the DATETIME values")

        // A conversion failure is materialized, not raised, so it persists into whatever table the
        // scan feeds. This is the shape that turns a read bug into stored bad data.
        def parquetUri = uriOf(outfile_to_S3("parquet"), "parquet")
        sql """ DROP TABLE IF EXISTS test_outfile_datetime_year_zero_restore """
        sql """
            CREATE TABLE test_outfile_datetime_year_zero_restore (
                `id` INT NOT NULL,
                `ts` DATETIME(6) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1");
        """
        sql """
            INSERT INTO test_outfile_datetime_year_zero_restore
            SELECT id, ts FROM ${s3Table(parquetUri, "parquet")}
        """
        def restored = sql """
            SELECT CAST(id AS STRING), CAST(ts AS STRING)
            FROM test_outfile_datetime_year_zero_restore ORDER BY id
        """
        assertEquals(expected, restored, "loading the exported file back stored different values")
    } finally {
        sql """ set time_zone = '${originalTimeZone}' """
        sql """ set enable_file_scanner_v2 = ${originalScannerV2} """
    }

}
