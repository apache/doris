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
import org.awaitility.Awaitility;
import static java.util.concurrent.TimeUnit.SECONDS;

suite("test_base_insert_job") {
    def tableName = "t_test_BASE_inSert_job"
    def jobName = "insert_recovery_test_base_insert_job"
    def jobMixedName = "Insert_recovery_Test_base_insert_job"
    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'JOB'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'DO'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'AT'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'SCHEDULE'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'STARTS'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'ENDS'
    """
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobMixedName}'
    """

    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
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
    Awaitility.await().atMost(30, SECONDS).until(
            {
                def onceJob = sql """ select SucceedTaskCount from jobs("type"="insert") where Name like '%${jobName}%' and ExecuteType='RECURRING' """
                println(onceJob)
                onceJob .size() == 1 && '1' <= onceJob.get(0).get(0)
                
            }
            )
    sql """
        PAUSE JOB where jobname =  '${jobName}'
    """
    def tblDatas = sql """select * from ${tableName}"""
    println tblDatas
    assert 3 >= tblDatas.size() >= (2 as Boolean) //at least 2 records, some times 3 records
    def pauseJobId = sql """select id from jobs("type"="insert") where Name='${jobName}'"""
    def taskStatus = sql """select status from tasks("type"="insert") where jobid= '${pauseJobId.get(0).get(0)}'"""
    println taskStatus
    for (int i = 0; i < taskStatus.size(); i++) {
        assert taskStatus.get(i).get(0) != "FAILED"||taskStatus.get(i).get(0) != "STOPPED"||taskStatus.get(i).get(0) != "STOPPED"
    }
    sql """
       CREATE JOB ${jobMixedName}  ON SCHEDULE every 1 second  DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """
    def mixedNameJobs = sql """select name,comment from jobs("type"="insert") where Name='${jobMixedName}'"""
    println mixedNameJobs
    assert mixedNameJobs.size() == 1 && mixedNameJobs.get(0).get(0) == jobMixedName
    assert mixedNameJobs.get(0).get(1) == ''
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobMixedName}'
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
    def dataCount = sql """select count(*) from ${tableName}"""
    assert dataCount.get(0).get(0) == 0
    sql """
          CREATE JOB ${jobName}  ON SCHEDULE at current_timestamp   comment 'test for test&68686781jbjbhj//ncsa' DO insert into ${tableName}  values  ('2023-07-19', 2, 1001);
     """

    Awaitility.await("create-one-time-job-test").atMost(30,SECONDS).until(
        {
            def onceJob = sql """ select SucceedTaskCount from jobs("type"="insert") where Name like '%${jobName}%' and ExecuteType='ONE_TIME' """
            onceJob.size() == 1 && '1' == onceJob.get(0).get(0)
        }
    )
    def onceJob = sql """ select SucceedTaskCount from jobs("type"="insert") where Name like '%${jobName}%' and ExecuteType='ONE_TIME' """
    assert onceJob.size() == 1
    //check succeed task count
    assert '1' == onceJob.get(0).get(0)
    def datas = sql """select status,taskid from tasks("type"="insert") where jobName= '${jobName}'"""
    println datas
    assert datas.size() == 1
    assert datas.get(0).get(0) == "FINISHED"
    // check table data
    def dataCount1 = sql """select count(1) from ${tableName}"""
    assert dataCount1.get(0).get(0) == 1
    // check job status
    def oncejob = sql """select status,comment from jobs("type"="insert") where Name='${jobName}' """
    println oncejob
    assert oncejob.get(0).get(0) == "FINISHED"
    //assert comment
    assert oncejob.get(0).get(1) == "test for test&68686781jbjbhj//ncsa"
    sql """
        DROP JOB IF EXISTS where jobname =  'press'
    """

    sql """
          CREATE JOB press  ON SCHEDULE every 10 hour starts CURRENT_TIMESTAMP  comment 'test for test&68686781jbjbhj//ncsa' DO insert into ${tableName}  values  ('2023-07-19', 99, 99);
     """
    Awaitility.await("create-immediately-job-test").atMost(60, SECONDS).until({
        def pressJob = sql """ select SucceedTaskCount from jobs("type"="insert") where name='press'"""
        println pressJob
        pressJob.size() == 1 && '1' == onceJob.get(0).get(0)
    })

    sql """
        DROP JOB IF EXISTS where jobname =  'past_start_time'
    """
    sql """
          CREATE JOB past_start_time  ON SCHEDULE every 10 hour starts '2023-11-13 14:18:07'  comment 'test for test&68686781jbjbhj//ncsa' DO insert into ${tableName}  values  ('2023-07-19', 99, 99);
     """

    def past_start_time_job = sql """ select status from jobs("type"="insert") where name='past_start_time'"""
    println past_start_time_job
    assert past_start_time_job.get(0).get(0) == "RUNNING"
    sql """
        DROP JOB IF EXISTS where jobname =  'past_start_time'
    """
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """
    sql """
          CREATE JOB ${jobName}  ON SCHEDULE every 1 second starts current_timestamp  comment 'test for test&68686781jbjbhj//ncsa' DO insert into ${tableName}  values  ('2023-07-19',5, 1001);
     """

    Awaitility.await("create-job-test").atMost(60, SECONDS).until({
        def job = sql """ select SucceedTaskCount from jobs("type"="insert") where name='${jobName}'"""
        println job
        job.size() == 1 && '1' <= job.get(0).get(0)
    })

    sql """
        PAUSE JOB where jobname =  '${jobName}'
    """
    def tasks = sql """ select status from tasks("type"="insert") where JobName= '${jobName}'  """
    sql """
        RESUME JOB where jobname =  '${jobName}'
    """
    println(tasks.size())
    Awaitility.await("resume-job-test").atMost(60, SECONDS).until({
        def afterResumeTasks = sql """ select status from tasks("type"="insert") where JobName= '${jobName}'   """
        println "resume tasks :"+afterResumeTasks
        afterResumeTasks.size() >tasks.size()
    })
   
    // assert same job name
    try {
        sql """
          CREATE JOB ${jobName}  ON SCHEDULE EVERY 10 second   comment 'test for test&68686781jbjbhj//ncsa' DO insert into ${tableName}  values  ('2023-07-19', 10, 1001);
     """
    } catch (Exception e) {
        assert e.getMessage().contains("job name exist, jobName:insert_recovery_test_base_insert_job")
    }
    // assert not support stmt
    try {
        sql """
            CREATE JOB ${jobName}  ON SCHEDULE at current_timestamp   comment 'test' DO update ${tableName} set type=2 where type=1;
        """
    } catch (Exception e) {
        assert e.getMessage().contains("Not support this sql")
    }
    // assert start time greater than current time
    try {
        sql """
            CREATE JOB ${jobName}  ON SCHEDULE at '2023-11-13 14:18:07'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
        """
    } catch (Exception e) {
        println e.getMessage()
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
        assert e.getMessage().contains("endTimeMs must be greater than the start time")
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

    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'JOB'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'DO'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'AT'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'SCHEDULE'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'STARTS'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  'ENDS'
    """
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobMixedName}'
    """

    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """

}
