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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.ZoneId;

suite("test_base_insert_job") {
    def tableName = "t_test_BASE_inSert_job"
    def jobName = "insert_recovery_test_base_insert_job"
    def jobMixedName = "Insert_recovery_Test_base_insert_job"
    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB where jobname =  '${jobName}'
    """
    sql """
       DROP JOB where jobname =  'JOB'
    """
    sql """
       DROP JOB where jobname =  'DO'
    """
    sql """
       DROP JOB where jobname =  'AT'
    """
    sql """
       DROP JOB where jobname =  'SCHEDULE'
    """
    sql """
       DROP JOB where jobname =  'STARTS'
    """
    sql """
       DROP JOB where jobname =  'ENDS'
    """
    sql """
        DROP JOB where jobname =  '${jobMixedName}'
    """

    sql """
        DROP JOB where jobname =  '${jobName}'
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}`
        (
            `timestamp` DATE NOT NULL COMMENT "['0000-01-01', '9999-12-31']",
            `type` TINYINT NOT NULL COMMENT "[-128, 127]",
            `user_id` BIGINT COMMENT "[-9223372036854775808, 9223372036854775807]"
        )
            DUPLICATE KEY(`timestamp`, `type`)
        DISTRIBUTED BY HASH(`type`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """
       CREATE JOB ${jobName}  ON SCHEDULE every 1 second   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """
    Thread.sleep(2500)
    def jobs = sql """select * from ${tableName}"""
    println jobs
    assert 3 >= jobs.size() >= (2 as Boolean) //at least 2 records, some times 3 records
    sql """
       CREATE JOB ${jobMixedName}  ON SCHEDULE every 1 second  DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """
    def mixedNameJobs = sql """select name,comment from jobs("type"="insert") where Name='${jobMixedName}'"""
    println mixedNameJobs
    assert mixedNameJobs.size() == 1 && mixedNameJobs.get(0).get(0) == jobMixedName
    assert mixedNameJobs.get(0).get(1) == ''
    sql """
        DROP JOB where jobname =  '${jobName}'
    """
    sql """
        DROP JOB where jobname =  '${jobMixedName}'
    """

    sql """drop table if exists `${tableName}` force """
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}`
        (
            `timestamp` DATE NOT NULL COMMENT "['0000-01-01', '9999-12-31']",
            `type` TINYINT NOT NULL COMMENT "[-128, 127]",
            `user_id` BIGINT COMMENT "[-9223372036854775808, 9223372036854775807]"
        )
            DUPLICATE KEY(`timestamp`, `type`)
        DISTRIBUTED BY HASH(`type`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    // Enlarge this parameter to avoid other factors that cause time verification to fail when submitting.
    def currentMs = System.currentTimeMillis() + 20000;
    def dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(currentMs), ZoneId.systemDefault());

    def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    def startTime = dateTime.format(formatter);
    def dataCount = sql """select count(*) from ${tableName}"""
    assert dataCount.get(0).get(0) == 0
    sql """
          CREATE JOB ${jobName}  ON SCHEDULE at '${startTime}'   comment 'test for test&68686781jbjbhj//ncsa' DO insert into ${tableName}  values  ('2023-07-19', sleep(10000), 1001);
     """

    Thread.sleep(25000)
    def onceJob = sql """ select id,ExecuteSql from jobs("type"="insert") where Name like '%${jobName}%' and ExecuteType='ONE_TIME' """
    assert onceJob.size() == 1
    def onceJobId = onceJob.get(0).get(0);
    def onceJobSql = onceJob.get(0).get(1);
    println onceJobSql
    def assertSql = "insert into ${tableName}  values  (\'2023-07-19\', sleep(10000), 1001);"
    println assertSql
    assert onceJobSql == assertSql
    // test cancel task
    def datas = sql """select status,taskid from tasks("type"="insert") where jobid= ${onceJobId}"""
    println datas
    assert datas.size() == 1
    assert datas.get(0).get(0) == "RUNNING"
    def taskId = datas.get(0).get(1)
    sql """cancel  task where jobName='${jobName}' and taskId= ${taskId}"""
    def cancelTask = sql """ select status from tasks("type"="insert") where jobid= ${onceJobId}"""
    println cancelTask
    //check task status
    assert cancelTask.size() == 1
    assert cancelTask.get(0).get(0) == "CANCELED"
    // check table data
    def dataCount1 = sql """select count(1) from ${tableName}"""
    assert dataCount1.get(0).get(0) == 0
    // check job status
    def oncejob = sql """select status,comment from jobs("type"="insert") where Name='${jobName}' """
    println oncejob
    assert oncejob.get(0).get(0) == "FINISHED"
    //assert comment
    assert oncejob.get(0).get(1) == "test for test&68686781jbjbhj//ncsa"
    sql """
        DROP JOB where jobname =  '${jobName}'
    """

    sql """
          CREATE JOB ${jobName}  ON SCHEDULE every 1 second   comment 'test for test&68686781jbjbhj//ncsa' DO insert into ${tableName}  values  ('2023-07-19', sleep(10000), 1001);
     """

    Thread.sleep(2500)

    sql """
        PAUSE JOB where jobname =  '${jobName}'
    """
    def job = sql """ select id,ExecuteSql from jobs("type"="insert") where Name like '%${jobName}%'  """
    assert job.size() == 1
    def jobId = job.get(0).get(0);
    def tasks = sql """ select status from tasks("type"="insert") where jobid= ${jobId}  """
    assert tasks.size() == 1
    sql """
        RESUME JOB where jobname =  '${jobName}'
    """
    Thread.sleep(2500)
    def resumeTasks = sql """ select status from tasks("type"="insert") where jobid= ${jobId}  """
    println resumeTasks
    assert resumeTasks.size() == 2
    // assert same job name
    try {
        sql """
          CREATE JOB ${jobName}  ON SCHEDULE EVERY 10 second   comment 'test for test&68686781jbjbhj//ncsa' DO insert into ${tableName}  values  ('2023-07-19', sleep(10000), 1001);
     """
    } catch (Exception e) {
        assert e.getMessage().contains("job name exist, jobName:insert_recovery_test_base_insert_job")
    }
    def errorTblName = "${tableName}qwertyuioppoiuyte"
    sql """drop table if exists `${errorTblName}` force"""
    // assert error table name
    try {
        sql """
          CREATE JOB ${jobName}  ON SCHEDULE EVERY 10 second   comment 'test for test&68686781jbjbhj//ncsa' DO insert into ${errorTblName}  values  ('2023-07-19', sleep(10000), 1001);
     """
    } catch (Exception e) {
        assert e.getMessage().contains("Unknown table 't_test_BASE_inSert_jobqwertyuioppoiuyte'")
    }
    // assert not support stmt
    try {
        sql """
            CREATE JOB ${jobName}  ON SCHEDULE at '${startTime}'   comment 'test' DO update ${tableName} set type=2 where type=1;
        """
    } catch (Exception e) {
        assert e.getMessage().contains("Not support UpdateStmt type in job")
    }
    // assert start time greater than current time
    try {
        sql """
            CREATE JOB ${jobName}  ON SCHEDULE at '${startTime}'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
        """
    } catch (Exception e) {
        assert e.getMessage().contains("startTimeMs must be greater than current time")
    }
    // assert end time less than start time
    try {
        sql """
            CREATE JOB test_one_time_error_starts  ON SCHEDULE at '2023-11-13 14:18:07'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
        """
    } catch (Exception e) {
        assert e.getMessage().contains("startTimeMs must be greater than current time")
    }
    try {
        sql """
            CREATE JOB inner_test  ON SCHEDULE at '2023-11-13 14:18:07'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
        """
    } catch (Exception e) {
        assert e.getMessage().contains("job name can not start with inner_")
    }
    // assert end time less than start time
    try {
        sql """
            CREATE JOB test_error_starts  ON SCHEDULE every 1 second ends '2023-11-13 14:18:07'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
        """
    } catch (Exception e) {
        assert e.getMessage().contains("end time cannot be less than start time")
    }
    // assert interval time unit can not be years
    try {
        sql """
            CREATE JOB test_error_starts  ON SCHEDULE every 1 years ends '2023-11-13 14:18:07'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
        """
    } catch (Exception e) {
        assert e.getMessage().contains("interval time unit can not be years")
    }

    // test keyword as job name
    sql """
        CREATE JOB JOB  ON SCHEDULE every 20 second   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """
    sql """
        CREATE JOB SCHEDULE  ON SCHEDULE every 20 second   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """
    sql """
        CREATE JOB DO  ON SCHEDULE every 20 second   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """
    sql """
        CREATE JOB AT  ON SCHEDULE every 20 second   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """

    sql """
        CREATE JOB STARTS  ON SCHEDULE every 20 second   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """

    sql """
        CREATE JOB ENDS  ON SCHEDULE every 20 second   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """

    def jobCountRsp = sql """select count(1) from jobs("type"="insert") where name in ('JOB','DO','SCHEDULE','AT','STARTS','ENDS')"""
    assert jobCountRsp.get(0).get(0) == 6

}
