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


import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_streaming_postgres_job_array_types", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_array_types_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_array_types_pg"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """
            CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                id                      bigserial PRIMARY KEY,
                int2_array_col          int2[],
                int4_array_col          int4[],
                int8_array_col          int8[],
                float4_array_col        float4[],
                double_array_col        double precision[],
                bool_array_col          bool[],
                varchar_array_col       varchar(50)[],
                text_array_col          text[],
                timestamp_array_col     timestamp[],
                timestamptz_array_col   timestamptz[]
            );
            """
            // mock snapshot data
            sql """
            INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (
                1,
                ARRAY[1::int2, 2::int2],
                ARRAY[10::int4, 20::int4],
                ARRAY[100::int8, 200::int8],
                ARRAY[1.1::float4, 2.2::float4],
                ARRAY[1.11::double precision, 2.22::double precision],
                ARRAY[true, false],
                ARRAY['foo'::varchar, 'bar'::varchar],
                ARRAY['hello', 'world'],
                ARRAY['2024-01-01 12:00:00'::timestamp, '2024-06-01 00:00:00'::timestamp],
                ARRAY['2024-01-01 12:00:00+08'::timestamptz, '2024-06-01 00:00:00+00'::timestamptz]
            );
            """
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}?timezone=UTC",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // check job running
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        jobSuccendCount.size() == 1 && '1' <= jobSuccendCount.get(0).get(0)
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        qt_desc_array_types """desc ${currentDb}.${table1};"""
        qt_select_array_types """select * from ${currentDb}.${table1} order by 1;"""

        // mock incremental data
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """
            INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (
                2,
                ARRAY[3::int2, 4::int2],
                ARRAY[30::int4, 40::int4],
                ARRAY[300::int8, 400::int8],
                ARRAY[3.3::float4, 4.4::float4],
                ARRAY[3.33::double precision, 4.44::double precision],
                ARRAY[false, true],
                ARRAY['baz'::varchar, 'qux'::varchar],
                ARRAY['foo', 'bar'],
                ARRAY['2025-01-01 06:00:00'::timestamp, '2025-06-01 18:00:00'::timestamp],
                ARRAY['2025-01-01 06:00:00+08'::timestamptz, '2025-06-01 18:00:00+00'::timestamptz]
            );
            """
        }

        sleep(60000); // wait for cdc incremental data

        qt_select_array_types2 """select * from ${currentDb}.${table1} order by 1;"""

        sql """
            DROP JOB IF EXISTS where jobname = '${jobName}'
        """

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
