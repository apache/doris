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

suite("test_cast_decimalv3_as_bool") {
     sql """ DROP TABLE IF EXISTS cast_decimalv3_as_bool """
     sql """
        CREATE TABLE IF NOT EXISTS cast_decimalv3_as_bool (
            `id` int(11) ,
            `k1` decimalv3(9,3) ,
            `k2` decimalv3(18,9) ,
            `k3` decimalv3(38,16) ,
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
        INSERT INTO cast_decimalv3_as_bool VALUES
        (1,0.00001,13131.2131321,0.000000000000000000),
        (2,0.00000,2131231.231,0.0000000023323),
        (3,3.141414,0.0000000000,123123.213123123132213);
    """
    qt_select1 """
        select k1,k2,k3 from cast_decimalv3_as_bool order by id
    """
    qt_select2 """
        select cast(k1 as boolean), cast(k2 as boolean) , cast(k3 as boolean) from cast_decimalv3_as_bool order by id
    """ 
    qt_select3"""
        select cast(3.00001 as boolean),  cast(cast(3.00001 as  boolean) as int),cast(0.001 as boolean),cast(0.000 as boolean);
    """
        qt_select3"""
        select cast(cast(3.00001 as double)as boolean),  cast(cast(cast(3.00001 as double) as  boolean) as int),cast(cast(0.001 as double) as boolean),cast(cast(0.000 as double) as boolean);
    """
}