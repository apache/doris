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

import org.junit.jupiter.api.Assertions;

suite("docs/data-operate/scheduler/job-scheduler.md", "p0,external,mysql,external_docker,external_docker_mysql") {
    def dropJobIfExists = { job_name ->
        sql "DROP JOB IF EXISTS where jobName='${job_name}'"
    }
    def createJobButMuteOutdateTime = { sqlText ->
        try {
            sql sqlText
        } catch (Exception e) {
            if (!e.getMessage().contains("startTimeMs must be greater than current time")) {
                throw e
            }
        }
    }

    try {
        multi_sql """
            create database if not exists db1;
            create table if not exists db1.tbl1 (
                create_time date
            ) properties ("replication_num" = "1");
            create database if not exists db2;
            create table if not exists db2.tbl2 (
                create_time date
            ) properties ("replication_num" = "1");
        """
        dropJobIfExists("my_job")
        createJobButMuteOutdateTime "CREATE JOB my_job ON SCHEDULE AT '2025-01-01 00:00:00' DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2"
        dropJobIfExists("my_job")
        createJobButMuteOutdateTime "CREATE JOB my_job ON SCHEDULE EVERY 1 DAY STARTS '2025-01-01 00:00:00' DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2 WHERE create_time >=  days_add(now(),-1)"
        dropJobIfExists("my_job")
        createJobButMuteOutdateTime "CREATE JOB my_job ON SCHEDULE EVERY 1 DAY STARTS '2025-01-01 00:00:00' ENDS '2026-01-01 00:10:00' DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2 WHERE create_time >=  days_add(now(),-1)"
        dropJobIfExists("my_job")
        createJobButMuteOutdateTime "CREATE JOB my_job ON SCHEDULE AT current_timestamp DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2"

        sql """
            CREATE TABLE IF NOT EXISTS user_activity (
              `user_id` LARGEINT NOT NULL COMMENT "User ID",
              `date` DATE NOT NULL COMMENT "Data import date",
              `city` VARCHAR(20) COMMENT "User city",
              `age` SMALLINT COMMENT "User age",
              `sex` TINYINT COMMENT "User gender",
              `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "Last visit date",
              `cost` BIGINT SUM DEFAULT "0" COMMENT "Total spending",
              `max_dwell_time` INT MAX DEFAULT "0" COMMENT "Max dwell time",
              `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "Min dwell time"
            ) AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
            DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
            );
        """

        if (enableJdbcTest()) {
            def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            def mysql_port = context.config.otherConfigs.get("mysql_57_port")
            String s3_endpoint = getS3Endpoint()
            String bucket = getS3BucketName()
            String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
            String driver_class = "com.mysql.cj.jdbc.Driver"

            multi_sql """
                DROP CATALOG IF EXISTS activity;
                CREATE CATALOG activity PROPERTIES (
                    "type"="jdbc",
                    "user"="root",
                    "password"="123456",
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "${driver_class}"
                );
                CALL EXECUTE_STMT("activity", "CREATE DATABASE IF NOT EXISTS user");
                CALL EXECUTE_STMT("activity", "DROP TABLE IF EXISTS user.activity");
                CALL EXECUTE_STMT("activity", "CREATE TABLE IF NOT EXISTS user.activity (
                    `user_id` INT NOT NULL,
                    `date` DATE NOT NULL,
                    `city` VARCHAR(20),
                    `age` SMALLINT,
                    `sex` TINYINT,
                    `last_visit_date` DATETIME DEFAULT '1970-01-01 00:00:00',
                    `cost` BIGINT DEFAULT '0',
                    `max_dwell_time` INT DEFAULT '0',
                    `min_dwell_time` INT DEFAULT '99999'
                )");
                CALL EXECUTE_STMT("activity", "INSERT INTO user.activity VALUES
                        (10000, '2017-10-01', 'Beijing', 20, 0, '2017-10-01 06:00:00', 20, 10, 10),
                        (10000, '2017-10-01', 'Beijing', 20, 0, '2017-10-01 07:00:00', 15, 2, 2),
                        (10001, '2017-10-01', 'Beijing', 30, 1, '2017-10-01 17:05:00', 2, 22, 22),
                        (10002, '2017-10-02', 'Shanghai', 20, 1, '2017-10-02 12:59:00', 200, 5, 5),
                        (10003, '2017-10-02', 'Guangzhou', 32, 0, '2017-10-02 11:20:00', 30, 11, 11),
                        (10004, '2017-10-01', 'Shenzhen', 35, 0, '2017-10-01 10:00:00', 100, 3, 3),
                        (10004, '2017-10-03', 'Shenzhen', 35, 0, '2017-10-03 10:20:00', 11, 6, 6)
                ");
            """

            dropJobIfExists("one_time_load_job")
            createJobButMuteOutdateTime """
            CREATE JOB one_time_load_job
              ON SCHEDULE
              AT '2024-8-10 03:00:00'
              DO
              INSERT INTO user_activity SELECT * FROM activity.user.activity
            """
            dropJobIfExists("schedule_load")
            createJobButMuteOutdateTime """
                CREATE JOB schedule_load
                  ON SCHEDULE EVERY 1 DAY
                  DO
                  INSERT INTO user_activity SELECT * FROM activity.user.activity where last_visit_date >= days_add(now(),-1)
            """
        }
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/scheduler/job-scheduler.md failed to exec, please fix it", t)
    }
}
