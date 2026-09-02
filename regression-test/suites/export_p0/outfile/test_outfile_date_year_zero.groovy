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

// Parquet and ORC both store DATE as days since 1970-01-01 in the proleptic Gregorian calendar,
// which differs from Doris's MySQL-calendar day number for 0000-01-01 .. 0000-02-28. The writer
// (which shares the Arrow date32 encoder) and the reader must therefore agree, otherwise a table
// exported and re-read by Doris comes back shifted by a day.
// See https://github.com/apache/doris/issues/67366
suite("test_outfile_date_year_zero", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName")

    def tableName = "test_outfile_date_year_zero_table"
    def outFilePath = "${bucket}/outfile/date_year_zero/exp_"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            `id` INT NOT NULL,
            `d` DATE NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    // 0000-01-01 .. 0000-02-28 are the window where the two calendars disagree; 0000-03-01 onwards
    // they coincide, so keep dates on both sides of the boundary plus the two range limits.
    sql """
        INSERT INTO ${tableName} VALUES
            (1, '0000-01-01'), (2, '0000-01-31'), (3, '0000-02-28'), (4, '0000-03-01'),
            (5, '0001-01-01'), (6, '1969-12-31'), (7, '1970-01-01'), (8, '2024-01-01'),
            (9, '9999-12-31'), (10, NULL);
    """

    def expected = [['1', '0000-01-01'], ['2', '0000-01-31'], ['3', '0000-02-28'],
                    ['4', '0000-03-01'], ['5', '0001-01-01'], ['6', '1969-12-31'],
                    ['7', '1970-01-01'], ['8', '2024-01-01'], ['9', '9999-12-31'],
                    ['10', null]]

    def source = sql """ SELECT CAST(id AS STRING), CAST(d AS STRING) FROM ${tableName} ORDER BY id """
    assertEquals(expected, source)

    def outfile_to_S3 = { format ->
        def res = sql """
            SELECT * FROM ${tableName} t
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

    for (String format : ["parquet", "orc"]) {
        def outfile_url = outfile_to_S3(format)
        def uri = "http://${bucket}.${s3_endpoint}" +
                outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1) + "0." + format
        def readBack = sql """
            SELECT CAST(id AS STRING), CAST(d AS STRING) FROM S3 (
                "uri" = "${uri}",
                "ACCESS_KEY" = "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${format}",
                "region" = "${region}"
            ) ORDER BY id;
        """
        assertEquals(expected, readBack, "${format} round trip changed the DATE values")
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
