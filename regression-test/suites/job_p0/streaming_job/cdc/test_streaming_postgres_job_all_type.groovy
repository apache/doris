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

suite("test_streaming_postgres_job_all_type", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_all_type_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_all_types_nullable_with_pk_pg"
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

        // create test
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // sql """CREATE SCHEMA IF NOT EXISTS ${pgSchema}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """
            create table ${pgDB}.${pgSchema}.${table1} (
                id                  bigserial PRIMARY KEY,
                smallint_col        smallint,
                integer_col         integer,
                bigint_col          bigint,
                real_col            real,
                double_col          double precision,
                numeric_col         numeric(20,6),
                char_col            char(10),
                varchar_col         varchar(255),
                text_col            text,
                boolean_col         boolean,
                date_col            date,
                time_col            time,
                timetz_col          time with time zone,
                timestamp_col       timestamp,
                timestamptz_col     timestamp with time zone,
                interval_col        interval,
                bytea_col           bytea,
                uuid_col            uuid,
                json_col            json,
                jsonb_col           jsonb,
                inet_col            inet,
                cidr_col            cidr,
                macaddr_col         macaddr,
                bit_col             bit(8),
                bit_varying_col     bit varying(16),
                int_array_col       integer[],
                text_array_col      text[],
                point_col           point
            );
            """
            // mock snapshot data
            sql """
            INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1,1,100,1000,1.23,4.56,12345.678901,'char','varchar','text value',true,'2024-01-01','12:00:00','12:00:00+08','2024-01-01 12:00:00','2024-01-01 12:00:00+08','1 day',decode('DEADBEEF', 'hex'),'11111111-2222-3333-4444-555555555555'::uuid,'{"a":1}','{"b":2}','192.168.1.1','192.168.0.0/24','08:00:2b:01:02:03',B'10101010',B'1010',ARRAY[1,2,3],ARRAY['a','b','c'],'(1,2)');
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
                        // check job status and succeed task count larger than 1
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

        qt_desc_all_types_null """desc ${currentDb}.${table1};"""
        qt_select_all_types_null """select * from ${currentDb}.${table1} order by 1;"""

        // mock incremental into
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2,2,200,2000,7.89,0.12,99999.000001,'char2','varchar2','another text',false,'2025-01-01','23:59:59','23:59:59+00','2025-01-01 23:59:59','2025-01-01 23:59:59+00','2 hours',decode('DEADBEEF', 'hex'),'11111111-2222-3333-4444-555555555556'::uuid,'{"x":10}','{"y":20}','10.0.0.1','10.0.0.0/16','08:00:2b:aa:bb:cc',B'11110000',B'1111',ARRAY[10,20],ARRAY['x','y'],'(3,4)');"""
        }

        sleep(60000); // wait for cdc incremental data

        // check incremental data
        qt_select_all_types_null2 """select * from ${currentDb}.${table1} order by 1;"""

        sql """
            DROP JOB IF EXISTS where jobname =  '${jobName}'
        """

        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
