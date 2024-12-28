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

suite("test_time_diff_microseconds") {
     sql """ DROP TABLE IF EXISTS tbl_time """
      sql """
        CREATE TABLE IF NOT EXISTS tbl_time (
        `id` int(11) ,
        `t1` DATETIMEV2(3) ,
        `t2` DATETIMEV2(4) 
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "enable_unique_key_merge_on_write" = "true",
        "replication_num" = "1"
        );
    """
    sql """
        INSERT INTO tbl_time VALUES(1,'0001-01-03 19:19:00.000000','0001-01-01 00:00:00.0000');
    """

     sql """
        INSERT INTO tbl_time VALUES(2,'0001-01-02 00:00:00.514514','0001-01-01 00:00:00.000000');
    """

    sql """
        INSERT INTO tbl_time VALUES(3,'0001-01-03 19:00:00.123123','0001-01-22 00:00:00.891312');
    """

    sql """set enable_fold_constant_by_be=false"""

    qt_select1 """
        select timediff(t1,t2) from tbl_time order by id
    """

    qt_select2 """
        select timediff(
            cast('0001-01-03 11:45:14' as Date ) , 
            cast('0001-01-01 00:00:00.000000' as Date));
    """

    qt_select3 """
        select timediff(
        cast('0001-01-03 00:00:00.114514' as Datetimev2(6) ) , 
        cast('0001-01-01 00:00:00.000000' as Datetimev2(6) ));
    """


    qt_select4 """
        select timediff(
        cast('0001-01-03 00:00:00.114514' as Datetimev2(3) ) , 
        cast('0001-01-01 00:00:00.000000' as Datetimev2(5) ));
    """

    sql """set enable_fold_constant_by_be=true"""

    qt_select5 """
        select timediff(t1,t2) from tbl_time order by id
    """

    qt_select6 """
        select timediff(
            cast('0001-01-03 11:45:14' as Date ) , 
            cast('0001-01-01 00:00:00.000000' as Date));
    """

    qt_select7 """
        select timediff(
        cast('0001-01-03 00:00:00.114514' as Datetimev2(6) ) , 
        cast('0001-01-01 00:00:00.000000' as Datetimev2(6) ));
    """


    qt_select8 """
        select timediff(
        cast('0001-01-03 00:00:00.114514' as Datetimev2(3) ) , 
        cast('0001-01-01 00:00:00.000000' as Datetimev2(5) ));
    """

    sql """ 
    select round(timediff(now(),'2024-08-15')/60/60,2);
    """

    qt_select9 """
       select timediff('0001-01-03 19:19:00.123','0001-01-01 00:00:00.0000');
    """
}
