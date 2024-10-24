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

suite("test_window_function_error") {
    sql """ set enable_nereids_planner = true; """
    sql """ set enable_fallback_to_original_planner = false; """
  

    sql """ DROP TABLE IF EXISTS win_func_error_db """
    sql """
         CREATE TABLE IF NOT EXISTS win_func_error_db (
              `k1` INT(11) NOT NULL  COMMENT "",
              `k2` INT(11) NOT NULL   COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        );
    """

    sql """
        insert into win_func_error_db values(1,0);
    """

    test {
        sql """
            select lag(k1,1,0) from win_func_error_db;
        """
        exception("analytic function is not allowed in LOGICAL_PROJECT")
    }
}
