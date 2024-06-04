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

suite("test_datetime_key") {
    sql "DROP TABLE IF EXISTS `test_datetime_key`"
    sql """
        create table `test_datetime_key` (
            `k1` datetimev1, `k2` int,
            INDEX idx_k1 (`k1`) USING BITMAP
        ) duplicate key(`k1`)
        distributed by hash(k2) buckets 3
        properties("replication_num" = "1");
    """
    sql """ insert into `test_datetime_key` values("2016-11-04 06:21:44", 1); """
    sql """ insert into `test_datetime_key` values("2016-11-05 12:41:14", 2); """
    sql """ insert into `test_datetime_key` values("2016-12-06 22:31:09", 3); """
    sql """ insert into `test_datetime_key` values("2016-12-07 23:59:59", 4); """
    sql """ insert into `test_datetime_key` values("2017-02-05 23:59:59", 5); """
    sql """ insert into `test_datetime_key` values("2017-08-22 00:00:01", 6); """
    sql """ insert into `test_datetime_key` values("2016-11-04 06:21:44", 1); """
    sql """ insert into `test_datetime_key` values("2016-11-05 12:41:14", 22); """
    sql """ insert into `test_datetime_key` values("2016-12-06 22:31:09", 3); """
    sql """ insert into `test_datetime_key` values("2016-12-07 23:59:59", 44); """
    sql """ insert into `test_datetime_key` values("2017-02-05 23:59:59", 5); """
    sql """ insert into `test_datetime_key` values("2017-08-22 00:00:01", 666); """

    qt_sql1 """
        select * from `test_datetime_key` where `k1` = "2016-11-04 06:21:44" order by `k1`, `k2`;
    """

    qt_sql2 """
        select * from `test_datetime_key` where `k1` <> "2016-11-05 12:41:14" order by `k1`, `k2`;
    """

    qt_sql3 """
        select * from `test_datetime_key` where `k1` = "2016-11-05 12:41:14" or `k1` = "2016-11-04 06:21:44" order by `k1`, `k2`;
    """

    qt_sql_in """
        select * from `test_datetime_key` where `k1` in ("2016-11-05 12:41:14", "2016-11-04 06:21:44", "2017-08-22 00:00:01", "2017-02-05 23:59:59")  order by `k1`, `k2`;
    """

    qt_sql_not_in """
        select * from `test_datetime_key` where `k1` not in ("2016-11-05 12:41:14", "2016-11-04 06:21:44", "2017-08-22 00:00:01", "2017-02-05 23:59:59")  order by `k1`, `k2`;
    """

    sql "DROP TABLE IF EXISTS `test_datetime_distributed`"
    sql """
        create table `test_datetime_distributed` (
            `k1` int, `k2` datetimev1
        ) duplicate key(`k1`)
        distributed by hash(k2) buckets 3
        properties("replication_num" = "1");
    """
    sql """ insert into `test_datetime_distributed` values(1, "2016-11-04 06:21:44"); """
    sql """ insert into `test_datetime_distributed` values(2, "2016-11-05 12:41:14"); """
    sql """ insert into `test_datetime_distributed` values(3, "2016-12-06 22:31:09"); """
    sql """ insert into `test_datetime_distributed` values(4, "2016-12-07 23:59:59"); """
    sql """ insert into `test_datetime_distributed` values(5, "2017-02-05 23:59:59"); """
    sql """ insert into `test_datetime_distributed` values(6, "2017-08-22 00:00:01"); """
    sql """ insert into `test_datetime_distributed` values(1, "2016-11-04 06:21:44"); """
    sql """ insert into `test_datetime_distributed` values(22, "2016-11-05 12:41:14");"""
    sql """ insert into `test_datetime_distributed` values(3, "2016-12-06 22:31:09"); """
    sql """ insert into `test_datetime_distributed` values(44, "2016-12-07 23:59:59");"""
    sql """ insert into `test_datetime_distributed` values(5, "2017-02-05 23:59:59"); """
    sql """ insert into `test_datetime_distributed` values(6, "2017-08-22 00:00:01"); """

    qt_sql_distribute_1 """
        select * from `test_datetime_distributed` where `k2` = "2016-11-04 06:21:44" order by `k1`, `k2`;
    """

    qt_sql_distribute_2 """
        select * from `test_datetime_distributed` where `k2` <> "2016-11-05 12:41:14" order by `k1`, `k2`;
    """

    qt_sql_distribute_3 """
        select * from `test_datetime_distributed` where `k2` = "2016-11-05 12:41:14" or `k2` = "2016-11-04 06:21:44" order by `k1`, `k2`;
    """

    qt_sql_distribute_in """
        select * from `test_datetime_distributed` where `k2` in ("2016-11-05 12:41:14", "2016-11-04 06:21:44", "2017-08-22 00:00:01", "2017-02-05 23:59:59")  order by `k1`, `k2`;
    """

    qt_sql_distribute_not_in """
        select * from `test_datetime_distributed` where `k2` not in ("2016-11-05 12:41:14", "2016-11-04 06:21:44", "2017-08-22 00:00:01", "2017-02-05 23:59:59")  order by `k1`, `k2`;
    """

    sql "DROP TABLE IF EXISTS `test_datetime_partition`"
    sql """
        create table `test_datetime_partition` (
            `k1` int, `k2` datetimev1,
            INDEX idx_k2 (`k2`) USING BITMAP
        ) duplicate key(`k1`)
        PARTITION BY range(`k2`)(
            PARTITION p_1610 VALUES [('2016-10-01 00:00:00'), ('2016-10-31 23:59:59')),
            PARTITION p_1611 VALUES [('2016-11-01 00:00:00'), ('2016-11-30 23:59:59')),
            PARTITION p_1612 VALUES [('2016-12-01 00:00:00'), ('2016-12-31 23:59:59')),
            PARTITION p_1702 VALUES [('2017-02-01 00:00:00'), ('2017-02-28 23:59:59')),
            PARTITION p_1708 VALUES [('2017-08-01 00:00:00'), ('2017-08-31 23:59:59'))
        )
        distributed by hash(`k1`) buckets 3
        properties(
            "replication_num" = "1"
        );
    """

    sql """ insert into `test_datetime_partition` values(1, "2016-11-04 06:21:44"); """
    sql """ insert into `test_datetime_partition` values(2, "2016-11-05 12:41:14"); """
    sql """ insert into `test_datetime_partition` values(3, "2016-12-06 22:31:09"); """
    sql """ insert into `test_datetime_partition` values(4, "2016-12-07 23:59:59"); """
    sql """ insert into `test_datetime_partition` values(5, "2017-02-05 23:59:59"); """
    sql """ insert into `test_datetime_partition` values(6, "2017-08-22 00:00:01"); """
    sql """ insert into `test_datetime_partition` values(1, "2016-11-04 06:21:44"); """
    sql """ insert into `test_datetime_partition` values(22, "2016-11-05 12:41:14");"""
    sql """ insert into `test_datetime_partition` values(3, "2016-12-06 22:31:09"); """
    sql """ insert into `test_datetime_partition` values(44, "2016-12-07 23:59:59");"""
    sql """ insert into `test_datetime_partition` values(5, "2017-02-05 23:59:59"); """
    sql """ insert into `test_datetime_partition` values(6, "2017-08-22 00:00:01"); """

    qt_sql_partition_1 """
        select * from `test_datetime_partition` where `k2` = "2016-11-04 06:21:44" order by `k1`, `k2`;
    """

    qt_sql_distribute_2 """
        select * from `test_datetime_partition` where `k2` <> "2016-11-05 12:41:14" order by `k1`, `k2`;
    """

    qt_sql_distribute_3 """
        select * from `test_datetime_partition` where `k2` = "2016-11-05 12:41:14" or `k2` = "2016-11-04 06:21:44" order by `k1`, `k2`;
    """

    qt_sql_distribute_in """
        select * from `test_datetime_partition` where `k2` in ("2016-11-05 12:41:14", "2016-11-04 06:21:44", "2017-08-22 00:00:01", "2017-02-05 23:59:59")  order by `k1`, `k2`;
    """

    qt_sql_distribute_not_in """
        select * from `test_datetime_partition` where `k2` not in ("2016-11-05 12:41:14", "2016-11-04 06:21:44", "2017-08-22 00:00:01", "2017-02-05 23:59:59")  order by `k1`, `k2`;
    """

    qt_join_rf """
        select *
        from `test_datetime_key` `t1`, `test_datetime_distributed` `t2`
        where `t1`.`k1` = `t2`.`k2` and `t1`.`k2` % 2 = 0 order by `t1`.`k1`, `t1`.`k2`, `t2`.`k1`, `t2`.`k2`;
    """

    qt_join_rf2 """
        select *
        from `test_datetime_key` `t1`, `test_datetime_distributed` `t2`
        where `t1`.`k1` = `t2`.`k2` and `t2`.`k1` % 2 = 0 order by `t1`.`k1`, `t1`.`k2`, `t2`.`k1`, `t2`.`k2`;
    """

    qt_join_rf3 """
        select *
        from `test_datetime_key` `t1`, `test_datetime_partition` `t2`
        where `t1`.`k1` = `t2`.`k2` and `t2`.`k1` % 2 = 0 order by `t1`.`k1`, `t1`.`k2`, `t2`.`k1`, `t2`.`k2`;
    """

    sql """
        delete from `test_datetime_key` where `k1` = '2016-12-06 22:31:09';
    """

    sql """
        delete from `test_datetime_distributed` where `k2` = '2016-12-06 22:31:09';
    """

    sql """
        delete from `test_datetime_partition` where `k2` = '2016-12-06 22:31:09';
    """

    qt_key_after_del """
        select * from `test_datetime_key` order by `k1`, `k2`;
    """

    qt_distributed_after_del """
        select * from `test_datetime_distributed` order by `k1`, `k2`;
    """

    qt_partition_after_del """
        select * from `test_datetime_partition` order by `k1`, `k2`;
    """
}
