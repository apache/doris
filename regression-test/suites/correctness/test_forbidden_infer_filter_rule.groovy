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

suite("test_forbidden_infer_filter_rule") {
    sql """ DROP TABLE IF EXISTS A """
    sql """
        CREATE TABLE IF NOT EXISTS A
        (
            a_id int
        )
        DISTRIBUTED BY HASH(a_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """ DROP TABLE IF EXISTS B """
    sql """
        CREATE TABLE IF NOT EXISTS B
        (
            b_id int
        )
        DISTRIBUTED BY HASH(b_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql " INSERT INTO A values (1), (-1);"

    sql " INSERT INTO B values (1);"
    
    qt_sql "select * from A left join B on a_id=b_id and b_id in (1,2) where a_id < 100 order by a_id;"
}
