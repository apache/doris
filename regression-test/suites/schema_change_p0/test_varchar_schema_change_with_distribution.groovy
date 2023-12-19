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
    def tableName = "test_varchar_schema_change_with_distribution"
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE;"""

    sql """
        CREATE TABLE ${tableName}
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
        sql """ alter table ${tableName} modify column watts_range varchar(30) """
        exception "Can not modify distribution column"
    }

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE;"""

}