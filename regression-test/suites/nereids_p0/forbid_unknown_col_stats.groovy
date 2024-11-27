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

suite("forbid_unknown_col_stats") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set forbid_unknown_col_stats=true;"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql "drop table if exists region"
    sql '''
    CREATE TABLE region  (
        r_regionkey      int NOT NULL,
        r_name       VARCHAR(25) NOT NULL,
        r_comment    VARCHAR(152)
    )ENGINE=OLAP
    unique KEY(`r_regionkey`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );
    '''
    
    test {
        sql """select r_regionkey from region;  """
        exception "java.sql.SQLException: errCode = 2, detailMessage = meet unknown column stats: r_regionkey";
    }
    
    sql "alter table region modify column r_regionkey set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='0', 'max_value'='4', 'row_count'='5');"
    sql """select r_regionkey from region;  """
}