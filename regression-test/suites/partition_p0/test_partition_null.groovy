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

suite("test_partition_null", "p0") {
    sql """DROP TABLE IF EXISTS agg_orders_list_number_crop_part"""
    sql """
            CREATE TABLE `agg_orders_list_number_crop_part` (
                  `o_orderkey` BIGINT NOT NULL,
                  `o_comment` VARCHAR(79) replace
                ) ENGINE=OLAP
                aggregate KEY(`o_orderkey`)
                COMMENT 'OLAP'
                partition by list (o_orderkey) (
                    PARTITION `p_1` VALUES IN ("1"),
                    PARTITION `p_2` VALUES IN ("2"),
                    PARTITION `p_3` VALUES IN ("3"),
                    PARTITION `p_4` VALUES IN ("4"),
                    PARTITION `pX` values in ((NULL))
                )
                DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                ); 
    """
    
    test {
        sql """
              insert into agg_orders_list_number_crop_part values (null,'yy');
        """
        exception "is not null, can't insert into NULL value"
    }

}
