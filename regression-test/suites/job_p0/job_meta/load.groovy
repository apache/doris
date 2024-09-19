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

suite('load', 'p0,restart_fe') {
    def tableName = "t_test_BASE_inSert_job"
    def oneTimeJobName = "JOB_ONETIME"
    def recurringJobName = "JOB_RECURRING"
    sql """drop table if exists `${tableName}` force"""
    sql """
       DROP JOB IF EXISTS where jobname =  '${oneTimeJobName}'
    """
    sql """
       DROP JOB IF EXISTS where jobname =  '${recurringJobName}'
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
       CREATE JOB ${recurringJobName}  ON SCHEDULE every 1 HOUR STARTS '2052-03-18 00:00:00'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """

    sql """
       CREATE JOB ${oneTimeJobName}  ON SCHEDULE  AT '2052-03-18 00:00:00'  comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """

}