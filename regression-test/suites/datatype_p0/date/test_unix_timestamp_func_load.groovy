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

suite("test_unix_timestamp_func_load") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()

    sql """
        CREATE TABLE IF NOT EXISTS t (
            id BIGINT
        )
        PROPERTIES("replication_num" = "1");
        """

    try {
        streamLoad {
            table "t"
            set 'format', 'json'
            set 'columns', 'a,b,id=unix_timestamp()'
            file 'data_by_line.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }

        def res1 = sql """
                        insert into t
                        select unix_timestamp();
                       """
        log.info("select frm s3 result: ${res1}".toString())
        assertTrue(res1.size() == 1)

        def label = "s3_load_default_" + UUID.randomUUID().toString().replaceAll("-", "")
        sql """
            LOAD LABEL ${label} (
                DATA INFILE("s3://${s3BucketName}/load/data_by_line.json")
                INTO TABLE t
                FORMAT AS "json"
                (a, b)
                SET (
                    id = unix_timestamp()
                )
            )
            WITH S3 (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            );
            """

            // Wait for load to complete
            def max_try_time = 60000
            while (max_try_time > 0) {
                def result = sql "SHOW LOAD WHERE label = '${label}'"
                if (result[0][2] == "FINISHED") {
                    break
                } else if (result[0][2] == "CANCELLED") {
                    throw new Exception("Load job cancelled: " + result[0][7])
                }
                Thread.sleep(1000)
                max_try_time -= 1000
                if (max_try_time <= 0) {
                    throw new Exception("Load job timeout")
                }
            }
    } finally {
        try_sql("DROP TABLE IF EXISTS t")
    }

}