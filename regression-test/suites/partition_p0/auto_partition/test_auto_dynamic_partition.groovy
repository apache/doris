
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

def var = suite("test_auto_dynamic_partition") {
    sql "drop table if exists auto_dynamic_partition_tb"
    sql """
            CREATE TABLE auto_dynamic_partition_tb
            (
                dd_date date not null comment '日期'
            )
                AUTO PARTITION BY RANGE (date_trunc(`dd_date`, 'day')) ()
            DISTRIBUTED BY HASH(dd_date) BUCKETS 1
            PROPERTIES
            (
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "day",
                "dynamic_partition.start" = "-60",
                "dynamic_partition.end" = "3",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "1",
                "replication_num" = "1"
            );
        """
    result1 = sql "show partitions from auto_dynamic_partition_tb"
    logger.info("${result1}")
    assertEquals(result1.size(), 4)
    sql "insert into auto_dynamic_partition_tb values (20240201)"
    result2 = sql "show partitions from auto_dynamic_partition_tb"
    partitionList = new ArrayList<String>()
    for (List<Object> items:result2){
        partitionList.add(String.valueOf(items.get(1)))
    }
    partitionNames = new ArrayList<String>()
    partitionNames.add("p20240201")
    partitionNames.add("p20240202")
    partitionNames.add("p20240203")
    partitionNames.add("p20240204")
    assertEquals(partitionList.containsAll(partitionNames), true)

}
