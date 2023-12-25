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

suite("test_from_millisecond_microsecond") {
     sql """ DROP TABLE IF EXISTS millimicro """
     sql """
        CREATE TABLE IF NOT EXISTS millimicro (
              `id` INT(11) NULL COMMENT ""   ,
              `t` BigINT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """

    sql """
        insert into millimicro values(1,1919810114514);
    """
    sql """
        insert into millimicro values(2,89417891234789);
    """
    sql """
        insert into millimicro values(3,1235817896941);
    """

    sql """
        insert into millimicro values(4,NULL);
    """
    sql """
        set enable_nereids_planner=false
    """

    qt_select1 """
        select 
        from_millisecond(t) as t1 , 
        microseconds_add(cast(from_unixtime(t/1000) as datetime(3)), cast((t % 1000) * 1000 as int)) as t2 
        from millimicro order by id;
    """

    qt_select2 """
        select 
        from_microsecond(t) as t1 , 
        microseconds_add(cast(from_unixtime(t/1000000) as datetime(6)), cast((t % 1000000) as int)) as t2 
        from millimicro order by id;
    """ 
    // 32536771199 is max valid timestamp for from_unixtime
    qt_select3 """
        select 
        from_unixtime(32536771199),     from_second(32536771199),
        from_unixtime(32536771199 + 1), from_second(32536771199 + 1),
        from_unixtime(21474836470),     from_second(21474836470);
    """ 

    qt_select4 """
        select 
        t,
        from_second(t), 
        second_timestamp(from_second(t))
        from millimicro order by id;
    """ 
    qt_select5 """
        select 
        t,
        from_millisecond(t), 
        millisecond_timestamp(from_millisecond(t))
        from millimicro order by id;
    """ 
    qt_select6 """
        select 
        t,
        from_microsecond(t), 
        microsecond_timestamp(from_microsecond(t))
        from millimicro order by id;
    """ 
    sql """
        set enable_nereids_planner=true,enable_fold_constant_by_be = false,forbid_unknown_col_stats = false
    """
   
    qt_select7 """
        select from_millisecond(t) as t1 from millimicro order by id;
    """
    qt_select8 """
        select from_microsecond(t) as t1 from millimicro order by id;
    """

    qt_select9 """
        select 
        FROM_UNIXTIME(2147483647),from_second(2147483647),
        FROM_UNIXTIME(2147483647 + 1),from_second(2147483647 + 1),
        FROM_UNIXTIME(21474836470),from_second(21474836470);
    """ 

    qt_select10 """
        select 
        t,
        from_second(t), 
        second_timestamp(from_second(t))
        from millimicro order by id;
    """ 
    qt_select11 """
        select 
        t,
        from_millisecond(t), 
        millisecond_timestamp(from_millisecond(t))
        from millimicro order by id;
    """ 
    qt_select12 """
        select 
        t,
        from_microsecond(t), 
        microsecond_timestamp(from_microsecond(t))
        from millimicro order by id;
    """ 
    qt_select13 """select SECOND_TIMESTAMP(cast('2023-11-18 00:09:32' as datetime));""" 
    qt_select14 """select MILLISECOND_TIMESTAMP(cast('2023-11-18 00:09:32' as datetime));""" 
    qt_select15 """select MICROSECOND_TIMESTAMP(cast('2023-11-18 00:09:32' as datetime));""" 
    sql """
        set enable_nereids_planner=false
    """
    qt_select16 """select SECOND_TIMESTAMP(cast('2023-11-18 00:09:32' as datetime));""" 
    qt_select17 """select MILLISECOND_TIMESTAMP(cast('2023-11-18 00:09:32' as datetime));""" 
    qt_select18 """select MICROSECOND_TIMESTAMP(cast('2023-11-18 00:09:32' as datetime));"""

    // not null 
    sql """ DROP TABLE IF EXISTS millimicro """
    sql """
        CREATE TABLE IF NOT EXISTS millimicro (
              `id` INT(11)  COMMENT ""   ,
              `t` BigINT  COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """

    sql """
        insert into millimicro values(1,1919810114514);
    """
    sql """
        insert into millimicro values(2,89417891234789);
    """
    sql """
        insert into millimicro values(3,1235817896941);
    """

    sql """
        insert into millimicro values(4,NULL);
    """


    qt_select1 """
        select 
        from_millisecond(t) as t1 , 
        microseconds_add(cast(from_unixtime(t/1000) as datetime(3)), cast((t % 1000) * 1000 as int)) as t2 
        from millimicro order by id;
    """

    qt_select2 """
        select 
        from_microsecond(t) as t1 , 
        microseconds_add(cast(from_unixtime(t/1000000) as datetime(6)), cast((t % 1000000) as int)) as t2 
        from millimicro order by id;
    """ 
    // 32536771199 is max valid timestamp for from_unixtime
    qt_select3 """
        select 
        from_unixtime(32536771199),     from_second(32536771199),
        from_unixtime(32536771199 + 1), from_second(32536771199 + 1),
        from_unixtime(21474836470),     from_second(21474836470);
    """ 

    qt_select4 """
        select 
        t,
        from_second(t), 
        second_timestamp(from_second(t))
        from millimicro order by id;
    """ 
    qt_select5 """
        select 
        t,
        from_millisecond(t), 
        millisecond_timestamp(from_millisecond(t))
        from millimicro order by id;
    """ 
    qt_select6 """
        select 
        t,
        from_microsecond(t), 
        microsecond_timestamp(from_microsecond(t))
        from millimicro order by id;
    """ 
    sql """
        set enable_nereids_planner=true,enable_fold_constant_by_be = false,forbid_unknown_col_stats = false
    """
   
    qt_select7 """
        select from_millisecond(t) as t1 from millimicro order by id;
    """
    qt_select8 """
        select from_microsecond(t) as t1 from millimicro order by id;
    """

    qt_select9 """
        select 
        FROM_UNIXTIME(2147483647),from_second(2147483647),
        FROM_UNIXTIME(2147483647 + 1),from_second(2147483647 + 1),
        FROM_UNIXTIME(21474836470),from_second(21474836470);
    """ 

    qt_select10 """
        select 
        t,
        from_second(t), 
        second_timestamp(from_second(t))
        from millimicro order by id;
    """ 
    qt_select11 """
        select 
        t,
        from_millisecond(t), 
        millisecond_timestamp(from_millisecond(t))
        from millimicro order by id;
    """ 
    qt_select12 """
        select 
        t,
        from_microsecond(t), 
        microsecond_timestamp(from_microsecond(t))
        from millimicro order by id;
    """ 

    // null datetime
    sql """ DROP TABLE IF EXISTS millimicro """
    sql """
        CREATE TABLE IF NOT EXISTS millimicro (
              `id` INT(11) NULL COMMENT ""   ,
              `t` Datetime(6) NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """

    sql """
        insert into millimicro values(1,'2023-01-01 00:00:00');
    """
    sql """
        insert into millimicro values(2,'2023-01-01 00:00:00.123');
    """
    sql """
        insert into millimicro values(3,'2023-01-01 00:00:00.123456');
    """

    qt_select_null_datetime """
        select 
        id,
        SECOND_TIMESTAMP(t),
        MILLISECOND_TIMESTAMP(t),
        MICROSECOND_TIMESTAMP(t)
        from millimicro
        order by id;
    """ 


    // not null datetime
    sql """ DROP TABLE IF EXISTS millimicro """
    sql """
        CREATE TABLE IF NOT EXISTS millimicro (
              `id` INT(11) NULL COMMENT ""   ,
              `t` Datetime(6)  COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """

    sql """
        insert into millimicro values(1,'2023-01-01 00:00:00');
    """
    sql """
        insert into millimicro values(2,'2023-01-01 00:00:00.123');
    """
    sql """
        insert into millimicro values(3,'2023-01-01 00:00:00.123456');
    """

    qt_select_not_null_datetime """
        select 
        id,
        SECOND_TIMESTAMP(t),
        MILLISECOND_TIMESTAMP(t),
        MICROSECOND_TIMESTAMP(t)
        from millimicro
        order by id;
    """ 

    sql " set time_zone='Asia/Shanghai' "
    qt_sql " select from_second(-1) "
    qt_sql " select from_microsecond(253402271999999999) "
    qt_sql " select from_microsecond(253402272000000000) "
}