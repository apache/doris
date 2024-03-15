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

suite("test_decimalv3_string") {
    def db = "test_decimalv3_string"
    sql "CREATE DATABASE IF NOT EXISTS ${db}"
    sql "use ${db}"
    sql "drop table if exists replicationtesttable"
    sql '''CREATE TABLE `replicationtesttable` (   `k1` varchar(144) NOT NULL COMMENT '_key__',   `k2` int(11) NOT NULL COMMENT '_key__',  v4 decimal(18,4) null, ) ENGINE=OLAP UNIQUE KEY(`k1`, `k2`) COMMENT 'OLAP' DISTRIBUTED BY HASH(`k1`) BUCKETS 32 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "is_being_synced" = "false", "storage_format" = "V2", "light_schema_change" = "true", "disable_auto_compaction" = "false", "enable_single_replica_compaction" = "false" ); '''
    sql "insert into replicationtesttable (k1,k2,v4) select 'aaa',10,22500000900.000000000000000000000000000000"
    sql "insert into replicationtesttable (k1,k2,v4) select 'bbb',20,'22500000900.000000000000000000000000000000'"

    qt_decimalv3_string "select * from replicationtesttable order by 1"
}
