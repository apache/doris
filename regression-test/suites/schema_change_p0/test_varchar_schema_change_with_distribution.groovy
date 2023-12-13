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

suite ("test_varchar_schema_change_with_distribution") {
    def aggTbName = "test_varchar_schema_change_with_distribution_agg"
    def dupTbName = "test_varchar_schema_change_with_distribution_dup"
    def uniqTbName = "test_varchar_schema_change_with_distribution_uniq"

    sql """ DROP TABLE IF EXISTS ${aggTbName} FORCE;"""
    sql """ DROP TABLE IF EXISTS ${dupTbName} FORCE;"""
    sql """ DROP TABLE IF EXISTS ${uniqTbName} FORCE;"""


    // agg table
    sql """
        CREATE TABLE ${aggTbName}
        (
            dt datetime NOT NULL COMMENT '分区日期',
            citycode SMALLINT,
            watts_range VARCHAR(20),
            pv BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(dt, citycode, watts_range)
        PARTITION BY RANGE(dt)()
        DISTRIBUTED BY HASH(dt, watts_range) BUCKETS 1
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="1",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="HOUR",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1"
        );
    """

    test {
        sql """ alter table ${aggTbName} modify column watts_range varchar(30) """
        exception "Can not modify distribution column"
    }

    sql """ DROP TABLE IF EXISTS ${aggTbName} FORCE;"""

    // uniq table
    sql """
        CREATE TABLE ${uniqTbName}
        (
            dt datetime NOT NULL COMMENT '分区日期',
            citycode SMALLINT,
            watts_range VARCHAR(20),
            pv BIGINT DEFAULT '0'
        )
        UNIQUE KEY(dt, citycode)
        PARTITION BY RANGE(dt)()
        DISTRIBUTED BY HASH(dt, watts_range) BUCKETS 1
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="1",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="HOUR",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1"
        );
    """

    test {
        sql """ alter table ${uniqTbName} modify column watts_range varchar(30) """
        exception "Can not modify distribution column"
    }
    sql """ DROP TABLE IF EXISTS ${uniqTbName} FORCE;"""

    // duplicate table
    sql """
        CREATE TABLE ${dupTbName}
        (
            dt datetime NOT NULL COMMENT '分区日期',
            citycode SMALLINT,
            watts_range VARCHAR(20),
            pv BIGINT DEFAULT '0'
        )
        DUPLICATE KEY(dt, citycode)
        PARTITION BY RANGE(dt)()
        DISTRIBUTED BY HASH(dt, watts_range) BUCKETS 1
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="1",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="HOUR",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1"
        );
    """

    test {
        sql """ alter table ${dupTbName} modify column watts_range varchar(30) """
        exception "Can not modify distribution column"
    }
    sql """ DROP TABLE IF EXISTS ${dupTbName} FORCE;"""

}