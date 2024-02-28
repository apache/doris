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
suite("test_alter_table_colocate") {
    sql """
        CREATE TABLE IF NOT EXISTS `t_event`
        (
            `@event_name` varchar(255) NOT NULL COMMENT '事件名称',
            `@event_time` datetime NOT NULL COMMENT '事件时间',
            `@dt` datetime NOT NULL COMMENT '分区时间',
            `@user_id` bigint NULL COMMENT '用户id',
            `@guid` varchar(255) NULL COMMENT '设备id',
            `@platform` varchar(100) NULL COMMENT '平台',
            `@ip` varchar(255) NULL COMMENT 'ip',
            `@country` varchar(255) NULL COMMENT '国家',
            `@province` varchar(255) NULL COMMENT '省份',
            `@city` varchar(255) NULL COMMENT '城市',
            `@isp` varchar(255) NULL COMMENT '运营商',
            `@country_code` varchar(255) NULL COMMENT '国家代码'
        ) ENGINE=OLAP
        DUPLICATE KEY(`@event_name`, `@event_time`)
        COMMENT '事件表'
        PARTITION BY RANGE(`@dt`)()
        DISTRIBUTED BY HASH(`@user_id`) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "HOUR",
            "dynamic_partition.end" = "5",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "10"
        );
    """
    sql """
    CREATE TABLE `t_user`
    (
        `@user_id`bigint(20) NOT NULL COMMENT '用户id',
        `@account`bigint(20) REPLACE_IF_NOT_NULL NULL COMMENT '数字账号',
        `@register_date`datetime REPLACE_IF_NOT_NULL NULL COMMENT '注册时间',
        `@gender`int(11) REPLACE_IF_NOT_NULL NULL COMMENT '性别'
    ) ENGINE=OLAP
    AGGREGATE KEY(`@user_id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`@user_id`) BUCKETS 10
    PROPERTIES (
        "colocate_with" = "group_xa_test_alter_table_colocate"
    );
    """

    sql """ALTER TABLE t_event SET ("colocate_with" = "group_xa_test_alter_table_colocate");"""
    def result = sql_return_maparray """SHOW PROC '/colocation_group'"""
    result.each {
        if(result.GroupName.contains("group_xa_test_alter_table_colocate")){
            assertEquals(result.IsStable, "true")
        }
    }
    sql "DROP TABLE IF EXISTS t_event FORCE"
    sql "DROP TABLE IF EXISTS t_user FORCE"
}
