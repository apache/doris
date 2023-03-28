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

suite("sort1") {

    sql """
DROP TABLE IF EXISTS a_test_table;
    """

    sql """
CREATE TABLE `a_test_table` (
  `time_period` datetime NOT NULL COMMENT 'data time',
  `area_name` varchar(255) NOT NULL COMMENT '',
  `province` varchar(255) NOT NULL COMMENT '',
  `res_name` varchar(255) NOT NULL COMMENT '',
  `device_name` varchar(255) NOT NULL COMMENT '',
  `vm_feed_dog_rate` decimal(10, 3) REPLACE_IF_NOT_NULL NULL COMMENT '',
  `feed_dog_clear_time` decimal(10, 3) REPLACE_IF_NOT_NULL NULL COMMENT '',
  `zero_flow_vm_count` decimal(10, 3) REPLACE_IF_NOT_NULL NULL COMMENT '',
  `top_ten_flow_vm` decimal(10, 3) REPLACE_IF_NOT_NULL NULL COMMENT '',
  `update_time` datetime REPLACE NULL COMMENT 'update time')
 ENGINE=OLAP
 AGGREGATE KEY(`time_period`, `area_name`, `province`, `res_name`, `device_name`
)COMMENT ''
PARTITION BY RANGE(`time_period`)(
PARTITION p20230319 VALUES [('2023-03-19 00:00:00'), ('2023-03-20 00:00:00')),
PARTITION p20230320 VALUES [('2023-03-20 00:00:00'), ('2023-03-21 00:00:00')),
PARTITION p20230321 VALUES [('2023-03-21 00:00:00'), ('2023-03-22 00:00:00')),
PARTITION p20230322 VALUES [('2023-03-22 00:00:00'), ('2023-03-23 00:00:00')),
PARTITION p20230323 VALUES [('2023-03-23 00:00:00'), ('2023-03-24 00:00:00')),
PARTITION p20230324 VALUES [('2023-03-24 00:00:00'), ('2023-03-25 00:00:00'))
)
DISTRIBUTED BY HASH(`time_period`, `area_name`, `province`, `res_name`, `device_name`) BUCKETS 4
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
)
    """

    sql """
insert into a_test_table values
('2023-03-21 07:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name1', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 07:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name2', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 07:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name3', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 07:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name4', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 07:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name5', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 07:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name6', 100, 100, 100, 100, '2023-03-21 17:00:00')
    """

    sql """
insert into a_test_table values
('2023-03-21 08:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name1', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 08:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name2', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 08:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name3', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 08:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name4', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 08:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name5', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 08:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name6', 100, 100, 100, 100, '2023-03-21 17:00:00')
    """

    sql """
insert into a_test_table values
('2023-03-21 09:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name1', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 09:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name2', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 09:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name3', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 09:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name4', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 09:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name5', 100, 100, 100, 100, '2023-03-21 17:00:00'),
('2023-03-21 09:00:00', 'Northeast', 'LiaoNing', 'NFV-D-DBSY-00A', 'device_name6', 100, 100, 100, 100, '2023-03-21 17:00:00')
    """

    test {
        sql """
select * from a_test_table where time_period BETWEEN '2023-03-21 00:00:00' AND '2023-03-21 23:30:59' order by time_period
    """
        rowNum 18
    }

    test {
        sql """
select * from a_test_table where time_period BETWEEN '2023-03-21 00:00:00' AND '2023-03-21 23:30:59' order by time_period asc 
    """
        rowNum 18
    }

    test {
        sql """
select * from a_test_table where time_period BETWEEN '2023-03-21 00:00:00' AND '2023-03-21 23:30:59' order by time_period desc 
    """
        rowNum 18
    }
}
