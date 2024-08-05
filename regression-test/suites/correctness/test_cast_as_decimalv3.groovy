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

suite("test_cast_as_decimalv3") {
     sql """ DROP TABLE IF EXISTS divtest """
     sql """
        CREATE TABLE IF NOT EXISTS divtest (
            `id` int(11) ,
            `val` decimalv3(16,2) 
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "enable_unique_key_merge_on_write" = "true",
        "replication_num" = "1"
        );
    """
    sql """
        set enable_nereids_planner=true,enable_fold_constant_by_be = false
    """
    sql """
        INSERT INTO divtest VALUES(1,3.00)
    """
    sql """
        INSERT INTO divtest VALUES(2,4.00)
    """
    sql """
        INSERT INTO divtest VALUES(3,5.00)
    """
    qt_select1 """
        select cast(1 as decimalv3(3,2)) / val from divtest order by id
    """
    qt_select2 """
        select cast(1 as DECIMALV3(5, 2)) /  cast(3 as DECIMALV3(5, 2))
    """ 
    qt_select3 """
        select 1.0 / val from divtest order by id
    """
    qt_select4 """
        select cast(-280.00000000 as decimal(16,4));
    """
    qt_select5 """
        select cast(-280.00005000 as decimal(16,4));
    """
}
