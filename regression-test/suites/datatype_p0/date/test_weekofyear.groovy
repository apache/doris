
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

suite("test_weekofyear") {
    sql "DROP TABLE IF EXISTS woy"
    
    sql """
        CREATE TABLE IF NOT EXISTS woy (
            c0 int,
            c1 varchar(10)       
            )
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0) BUCKETS 1 properties("replication_num" = "1")
        """
    sql "insert into woy values(20000101, '2000-01-01')"
    sql "insert into woy values(20000110, '2000-01-10')"


    sql "set enable_nereids_planner=true"
    sql "set enable_vectorized_engine=true"
    qt_weekOfYear "select * from woy where weekofyear(c0)=52 and weekofyear(c1)=52"
}
