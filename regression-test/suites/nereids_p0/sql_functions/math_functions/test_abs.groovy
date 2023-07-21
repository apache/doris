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

suite("test_abs") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """DROP TABLE IF EXISTS `tpch_tiny_lineitem`; """
    sql """ 
        CREATE TABLE IF NOT EXISTS tpch_tiny_lineitem (
            orderkey bigint,
            quantity double
        ) DUPLICATE KEY(orderkey) DISTRIBUTED BY HASH(orderkey) BUCKETS 3 PROPERTIES ("replication_num" = "1")
    """
    sql """insert into tpch_tiny_lineitem values(1, 1.0); """
    qt_select """select abs(cast(abs(cast(76 as int)) as int)) as c1 from tpch_tiny_lineitem as ref_1 order by ref_1.`quantity` desc;"""
    sql """DROP TABLE IF EXISTS `tpch_tiny_lineitem`; """
}

