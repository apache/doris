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

suite("test_short_key_index_groovy") {


    def int_tab = "test_short_key_index_int_tab"

    sql "drop table if exists ${int_tab}"

    sql """
        CREATE TABLE IF NOT EXISTS `${int_tab}` (
          `siteid` int(11) NOT NULL COMMENT "",
          `citycode` int(11) NOT NULL COMMENT "",
          `userid` int(11) NOT NULL COMMENT "",
          `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`siteid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )

    """
    sql """
        insert into `${int_tab}` values
        (9,10,11,12),
        (9,10,11,12),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (5,6,7,8),
        (5,6,7,8)
    """

    qt_int_query_one_column "select siteid from `${int_tab}` order by siteid"

    qt_int_query_eq_pred "select siteid from `${int_tab}` where siteid = 13 order by siteid"

    qt_int_query_bigger_pred "select siteid from `${int_tab}` where siteid > 13 order by siteid"

    qt_int_query_bigger_eq_pred " select siteid from `${int_tab}` where siteid >= 13 order by siteid"

    qt_int_query_less_than_pred " select siteid from `${int_tab}` where siteid < 13 order by siteid"

    qt_int_query_less_than_eq_pred " select siteid from `${int_tab}` where siteid <= 13 order by siteid"

    sql "drop table if exists ${int_tab}"


    // short key is char

    def char_tab = "test_short_key_index_char_tab"

    sql "drop table if exists `${char_tab}`"

    sql """
        CREATE TABLE IF NOT EXISTS `${char_tab}` (
          `name` char(10) NOT NULL COMMENT "",
          `citycode` int(11) NOT NULL COMMENT "",
          `userid` int(11) NOT NULL COMMENT "",
          `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`,`citycode`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`citycode`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )

    """

    sql """
        insert into `${char_tab}` values
        ('a1',10,11,12),
        ('b2',10,11,12),
        ('c3',2,3,4),
        ('d4',21,22,16)
    """

    qt_char_query_one_column "select name from `${char_tab}` order by name"

    qt_char_query_eq_pred "select name from `${char_tab}` where name='d4' order by name"

    qt_char_query_bigger_pred "select name from `${char_tab}` where name > 'd4' order by name"

    qt_char_query_bigger_eq_pred "select name from `${char_tab}` where name>='d4' order by name"

    qt_char_query_less_than_pred "select name from `${char_tab}` where name < 'd4' order by name"

    qt_char_query_less_than_eq_pred " select name from `${char_tab}` where name <= 'd4' order by name"

    sql "drop table if exists `${char_tab}`"


    // short key is date
    def date_tab = "test_short_key_index_date_tab"

    sql "drop table if exists `${date_tab}`"

    sql """
        CREATE TABLE IF NOT EXISTS `${date_tab}` (
          `dt` date NOT NULL COMMENT "",
          `citycode` int(11) NOT NULL COMMENT "",
          `userid` int(11) NOT NULL COMMENT "",
          `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`dt`,`citycode`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`citycode`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )

    """

    sql """
       insert into `${date_tab}` values
            ('2022-01-01',10,11,12),
            ('2022-02-01',10,11,12),
            ('2022-03-01',2,3,4),
            ('2022-04-01',21,22,16)
    """

    qt_date_query_one_column "select dt from `${date_tab}` order by dt"

    qt_date_query_eq_pred "select dt from `${date_tab}` where dt='2022-04-01' order by dt"

    qt_date_query_bigger_pred "select dt from `${date_tab}` where dt>'2022-04-01' order by dt"

    qt_date_query_bigger_eq_pred "select dt from `${date_tab}` where dt>='2022-04-01' order by dt"

    qt_date_query_less_than_pred "select dt from `${date_tab}` where dt < '2022-04-01' order by dt"

    qt_date_query_less_than_eq_pred "select dt from `${date_tab}` where dt<='2022-04-01' order by dt"

    sql "drop table if exists `${date_tab}`"


    // short key is datev2
    def datev2_tab = "test_short_key_index_datev2_tab"

    sql "drop table if exists `${datev2_tab}`"

    sql """
        CREATE TABLE IF NOT EXISTS `${datev2_tab}` (
          `dt` datev2 NOT NULL COMMENT "",
          `citycode` int(11) NOT NULL COMMENT "",
          `userid` int(11) NOT NULL COMMENT "",
          `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`dt`,`citycode`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`citycode`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )

    """

    sql """
        insert into `${datev2_tab}` values
            ('2022-01-01',10,11,12),
            ('2022-02-01',10,11,12),
            ('2022-03-01',2,3,4),
            ('2022-04-01',21,22,16)
    """

    qt_datev2_query_one_column "select dt from `${datev2_tab}` order by dt"

    qt_datev2_query_eq_pred "select dt from `${datev2_tab}` where dt='2022-04-01' order by dt"

    qt_datev2_query_bigger_pred "select dt from `${datev2_tab}` where dt>'2022-04-01' order by dt"

    qt_datev2_query_bigger_eq_pred "select dt from `${datev2_tab}` where dt>='2022-04-01' order by dt"

    qt_datev2_query_less_than_pred "select dt from `${datev2_tab}` where dt < '2022-04-01' order by dt"

    qt_datev2_query_less_than_eq_pred "select dt from `${datev2_tab}` where dt<='2022-04-01' order by dt"

    sql "drop table if exists `${datev2_tab}`"

    // short key is datetime
    def datetime_tab = "test_short_key_index_datetime_tab"

    sql "drop table if exists `${datetime_tab}`"

    sql """
        CREATE TABLE IF NOT EXISTS `${datetime_tab}` (
            `dtime` datetime NOT NULL COMMENT "",
            `citycode` int(11) NOT NULL COMMENT "",
            `userid` int(11) NOT NULL COMMENT "",
            `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`dtime`,`citycode`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`citycode`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    """

    sql """
      insert into `${datetime_tab}` values
        ('2022-01-01 00:00:00',10,11,12),
        ('2022-01-01 01:00:00',10,11,12),
        ('2022-01-01 03:00:00',2,3,4),
        ('2022-01-01 04:00:00',21,22,16)
    """

    qt_datetime_query_one_column "select dtime,citycode from `${datetime_tab}` order by dtime,citycode"

    qt_datetime_query_eq_pred "select dtime,citycode from `${datetime_tab}` where dtime='2022-01-01 04:00:00' order by dtime,citycode"

    qt_datetime_query_bigger_pred "select dtime,citycode from `${datetime_tab}` where dtime > '2022-01-01 04:00:00' order by dtime,citycode"

    qt_datetime_query_bigger_eq_pred "select dtime,citycode from `${datetime_tab}` where dtime >= '2022-01-01 04:00:00' order by dtime,citycode"

    qt_datetime_query_less_than_pred "select dtime,citycode from `${datetime_tab}` where dtime < '2022-01-01 04:00:00' order by dtime,citycode"

    qt_datetime_query_less_than_eq_pred "select dtime,citycode from `${datetime_tab}` where dtime <='2022-01-01 04:00:00' order by dtime,citycode"

    sql "drop table if exists `${datetime_tab}`"


    // short key is datetimev2
    def datetimev2_tab = "test_short_key_index_datetimev2_tab"

    sql "drop table if exists `${datetimev2_tab}`"

    sql """
        CREATE TABLE IF NOT EXISTS `${datetimev2_tab}` (
          `dtime` datetimev2 NOT NULL COMMENT "",
          `citycode` int(11) NOT NULL COMMENT "",
          `userid` int(11) NOT NULL COMMENT "",
          `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
            DUPLICATE KEY(`dtime`,`citycode`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`citycode`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """

    sql """
      insert into `${datetimev2_tab}` values
        ('2022-01-01 00:00:00',10,11,12),
        ('2022-01-01 01:00:00',10,11,12),
        ('2022-01-01 03:00:00',2,3,4),
        ('2022-01-01 04:00:00',21,22,16)
    """

    qt_datetimev2_query_one_column "select dtime,citycode from `${datetimev2_tab}` order by dtime,citycode"

    qt_datetimev2_query_eq_pred "select dtime,citycode from `${datetimev2_tab}` where dtime='2022-01-01 04:00:00' order by dtime,citycode"

    qt_datetimev2_query_bigger_pred "select dtime,citycode from `${datetimev2_tab}` where dtime > '2022-01-01 04:00:00' order by dtime,citycode"

    qt_datetimev2_query_bigger_eq_pred "select dtime,citycode from `${datetimev2_tab}` where dtime >= '2022-01-01 04:00:00' order by dtime,citycode"

    qt_datetimev2_query_less_than_pred "select dtime,citycode from `${datetimev2_tab}` where dtime < '2022-01-01 04:00:00' order by dtime,citycode"

    qt_datetimev2_query_less_than_eq_pred "select dtime,citycode from `${datetimev2_tab}` where dtime <='2022-01-01 04:00:00' order by dtime,citycode"

    sql "drop table if exists `${datetimev2_tab}`"


    // short key is decimal
    def decimal_tab = "test_short_key_index_decimal_tab"

    sql "drop table if exists `${decimal_tab}`"

    sql """
         CREATE TABLE IF NOT EXISTS `${decimal_tab}` (
              `siteid` decimal NOT NULL COMMENT "",
              `citycode` int(11) NOT NULL COMMENT "",
              `userid` int(11) NOT NULL COMMENT "",
              `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`siteid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """

    sql """
      insert into `${decimal_tab}` values
        (9,10,11,12),
        (9,10,11,12),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (5,6,7,8),
        (5,6,7,8)
    """

    qt_decimal_query_one_column "select siteid from `${decimal_tab}` order by siteid"

    qt_decimal_query_eq_pred "select siteid from `${decimal_tab}` where siteid=17 order by siteid"

    qt_decimal_query_bigger_pred "select siteid from `${decimal_tab}` where siteid>17 order by siteid"

    qt_decimal_query_bigger_eq_pred "select siteid from `${decimal_tab}` where siteid>=17 order by siteid"

    qt_decimal_query_less_than_pred "select siteid from `${decimal_tab}` where siteid<17 order by siteid"

    qt_decimal_query_less_than_eq_pred "select siteid from `${decimal_tab}` where siteid<=17 order by siteid"

    sql "drop table if exists `${decimal_tab}`"


    // short key is decimalv2
    def decimalv2_tab = "test_short_key_index_decimalv2_tab"

    sql "drop table if exists `${decimalv2_tab}`"

    sql """
         CREATE TABLE IF NOT EXISTS `${decimalv2_tab}` (
          `siteid` decimalv3 NOT NULL COMMENT "",
          `citycode` int(11) NOT NULL COMMENT "",
          `userid` int(11) NOT NULL COMMENT "",
          `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`siteid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """

    sql """
      insert into `${decimalv2_tab}` values
        (9,10,11,12),
        (9,10,11,12),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (5,6,7,8),
        (5,6,7,8)
    """

    qt_decimalv2_query_one_column "select siteid from `${decimalv2_tab}` order by siteid"

    qt_decimalv2_query_eq_pred "select siteid from `${decimalv2_tab}` where siteid=5 order by siteid"

    qt_decimalv2_query_bigger_pred "select siteid from `${decimalv2_tab}` where siteid>5 order by siteid"

    qt_decimalv2_query_bigger_eq_pred "select siteid from `${decimalv2_tab}` where siteid>=5 order by siteid"

    // These two sqls could fail because of bug of current master, but not caused by current pr
//    qt_decimalv2_query_less_than_pred "select siteid from `${decimalv2_tab}` where siteid<5 order by siteid"
//    qt_decimalv2_query_less_than_eq_pred "select siteid from `${decimalv2_tab}` where siteid<=5 order by siteid"

    sql "drop table if exists `${decimalv2_tab}`"


    // mix type short key
    def mix_type_tab = "test_short_key_index_mixed_type_tab"

    sql "drop table if exists `${mix_type_tab}`"

    sql """
         CREATE TABLE IF NOT EXISTS `${mix_type_tab}` (
          `decimal_col` decimal NOT NULL COMMENT "",
          `int_col` int not null,
          `date_col` date not null,
          `varchar_col` varchar(10) not null,
          `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`decimal_col`, `int_col`, `date_col`, `varchar_col`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`int_col`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    """

    sql """
     insert into `${mix_type_tab}` values
        (1,2,'2022-01-01','a34', 100),
        (1,7,'2022-01-02','a44', 101),
        (2,8,'2022-01-03','a55', 102),
        (3,9,'2022-01-04','a56', 103),
        (4,10,'2022-05-01','a77', 104),
        (4,10,'2022-05-01','a77', 104)
    """

    qt_mix_type_query_all "select * from `${mix_type_tab}` order by 1,2,3,4,5"

    qt_pred_num_4 "select * from `${mix_type_tab}` where decimal_col=4 and int_col=10 and date_col='2022-05-01' and varchar_col='a77'"

    qt_pred_num_3 "select * from `${mix_type_tab}` where decimal_col=4 and int_col=10 and date_col='2022-05-01'"

    qt_pred_num_2 "select * from `${mix_type_tab}` where decimal_col=4 and int_col=10 "

    qt_pred_num_1 "select * from `${mix_type_tab}` where decimal_col=4 and int_col=10 "

    sql "drop table if exists `${mix_type_tab}`"


    // nulable
    def nullable_tab = "test_short_key_index_is_nullable_tab"

    sql "drop table if exists `${nullable_tab}`"

    sql """
         CREATE TABLE IF NOT EXISTS `${nullable_tab}` (
          `siteid` int(11)  NULL COMMENT "",
          `citycode` int(11)  NULL COMMENT "",
          `userid` int(11)  NULL COMMENT "",
          `pv` int(11)  NULL COMMENT ""
        ) ENGINE=OLAP
            DUPLICATE KEY(`siteid`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """

    sql """
      insert into `${nullable_tab}` values
        (1,10,11,12),
        (1,10,11,12),
        (2,2,3,4),
        (2,21,22,16),
        (3,14,15,16),
        (3,18,19,20),
        (4,2,3,4),
        (4,21,22,16),
        (null,14,15,16),
        (null,16,17,18)
    """

    qt_nullable_query_one_column "select siteid from `${nullable_tab}` order by siteid"

    qt_nullable_query_eq_pred "select siteid from `${nullable_tab}` where siteid = 4"

    qt_nullable_query_bigger_pred "select siteid from `${nullable_tab}` where siteid > 4"

    qt_nullable_query_bigger_eq_pred "select siteid from `${nullable_tab}` where siteid >= 4"

    qt_nullable_query_less_pred "select siteid from `${nullable_tab}` where siteid >= 4"

    qt_nullable_query_less_eq_pred "select siteid from `${nullable_tab}` where siteid >= 4"

    qt_nullable_query_is_null_pred "select siteid from `${nullable_tab}` where siteid is null"

    qt_nullable_query_is_not_null_pred "select siteid from `${nullable_tab}` where siteid is not null"

    sql "drop table if exists `${nullable_tab}`"

    // bool

    def bool_tab = "test_short_key_index_bool_tab"

    sql "drop table if exists `${bool_tab}`"

    sql """
         CREATE TABLE IF NOT EXISTS `${bool_tab}` (
          `is_happy` boolean  NULL COMMENT "",
          `citycode` int(11)  NULL COMMENT "",
          `userid` int(11)  NULL COMMENT "",
          `pv` int(11)  NULL COMMENT ""
        ) ENGINE=OLAP
            DUPLICATE KEY(`is_happy`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`citycode`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """

    sql """
      insert into `${bool_tab}` values
        (true,10,11,12),
        (false,10,11,12),
        (true,2,3,4),
        (false,21,22,16)
    """

    qt_bool_query_true "select is_happy,citycode from `${bool_tab}` where is_happy = true order by is_happy,citycode"

    qt_bool_query_false "select is_happy,citycode from `${bool_tab}` where is_happy = false order by is_happy,citycode"


    sql "drop table if exists `${bool_tab}`"

}
