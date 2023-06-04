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

suite("test_session_dry_run_fe")
     sql """ DROP TABLE IF EXISTS test_session_dry_run_fe """
      sql """ 
        CREATE TABLE test_session_dry_run_fe (
            k1 INT DEFAULT '1',
            k2 VARCHAR(32) DEFAULT ''
        ) ENGINE=OLAP
        AGGREGATE KEY(k1,k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 10
        PROPERTIES (
         "replication_allocation" = "tag.location.default: 1",
         "in_memory" = "false",
         "storage_format" = "V2"
        );
    """
     sql """
        insert into test_session_dry_run_fe values(100,'20:20:20')
    """
    sql """
        set dry_run_fe = true
    """
    qt_select1 """
        select k1 from test_session_dry_run_fe
    """
