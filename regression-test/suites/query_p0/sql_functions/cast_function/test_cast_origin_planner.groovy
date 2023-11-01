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

suite("test_cast_origin_planner") {
    sql """ set enable_nereids_planner=false """
    sql " drop table if exists t1"
    sql " drop table if exists t2"
    sql """ create table t1 PROPERTIES("replication_num"="1") as select 1.0 v """
    sql """ create table t2 PROPERTIES("replication_num"="1") as select 1.0 v """
    sql """ select cast ("0.0000031417" as datetime) """
    qt_sql """ select * from t1 join t2 on trim(t1.v + t2.v) where cast(1 as boolean) """
}

