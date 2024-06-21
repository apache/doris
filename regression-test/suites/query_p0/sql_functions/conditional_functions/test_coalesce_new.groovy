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

suite("test_coalesce_new") {
    // test parameter:datetime, datev2
    sql """
        admin set frontend config ("enable_date_conversion"="false")
        """
    sql """
        admin set frontend config ("disable_datev1"="false")
        """
    sql """
        drop table if exists test_cls
        """

    sql """
            CREATE TABLE `test_cls` (
                `id` int(11) NOT NULL COMMENT '',
                `name` varchar(32) NOT NULL COMMENT '',
                `dt` datetime NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES(
                "replication_allocation" = "tag.location.default: 1"
            );  
        """

    sql """
            insert into test_cls values (1,'Alice','2023-06-01 12:00:00'),(2,'Bob','2023-06-02 12:00:00'),(3,'Carl','2023-05-01 14:00:00')
    """

    sql """
        SET enable_nereids_planner=false
        """
    def result1 = try_sql """
        select dt from test_cls where coalesce (dt, str_to_date(concat('202306', '01'), '%Y%m%d')) >= '2023-06-01'
    """
    assertEquals(result1.size(), 2);
    def result11 = try_sql """
        select dt from test_cls where coalesce (dt, dt, str_to_date(concat('202306', '01'), '%Y%m%d')) >= '2023-06-01'
    """
    assertEquals(result11.size(), 2);
    def result12 = try_sql """
        select dt from test_cls where coalesce (dt, str_to_date(concat('202306', '01'), '%Y%m%d'), str_to_date(concat('202306', '01'), '%Y%m%d')) >= '2023-06-01'
    """
    assertEquals(result12.size(), 2);


    // test parameter:datetimev2, datev2
    sql """
        admin set frontend config ("enable_date_conversion"="true")
        """
    sql """
        admin set frontend config ("disable_datev1"="true")
        """
    sql """
        drop table if exists test_cls_dtv2
        """

    sql """
            CREATE TABLE `test_cls_dtv2` (
                `id` int(11) NOT NULL COMMENT '',
                `name` varchar(32) NOT NULL COMMENT '',
                `dt` datetime NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES(
                "replication_allocation" = "tag.location.default: 1"
            );  
        """

    sql """
            insert into test_cls_dtv2 values (1,'Alice','2023-06-01 12:00:00'),(2,'Bob','2023-06-02 12:00:00'),(3,'Carl','2023-05-01 14:00:00')
    """

    sql """
        SET enable_nereids_planner=false
        """
    def result2 = try_sql """
        select dt from test_cls_dtv2 where coalesce (dt, str_to_date(concat('202306', '01'), '%Y%m%d')) >= '2023-06-01'
    """
    assertEquals(result2.size(), 2);
    def result21 = try_sql """
        select dt from test_cls_dtv2 where coalesce (dt, dt, str_to_date(concat('202306', '01'), '%Y%m%d')) >= '2023-06-01'
    """
    assertEquals(result21.size(), 2);
    def result22 = try_sql """
        select dt from test_cls_dtv2 where coalesce (dt, str_to_date(concat('202306', '01'), '%Y%m%d'), str_to_date(concat('202306', '01'), '%Y%m%d')) >= '2023-06-01'
    """
    assertEquals(result22.size(), 2);


    sql """
        drop table test_cls
        """
    sql """
        drop table test_cls_dtv2
        """
    sql """
        admin set frontend config ("disable_datev1"="false")
        """
    sql """
        admin set frontend config ("enable_date_conversion"="true")
        """
}
