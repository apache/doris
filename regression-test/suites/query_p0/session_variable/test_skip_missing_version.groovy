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

suite("test_skip_missing_version") {
    def test_tbl = "test_skip_missing_version_tbl"

    sql """ DROP TABLE IF EXISTS ${test_tbl}"""
    sql """
     CREATE TABLE ${test_tbl} (
       `k1` int(11) NULL,
       `k2` char(5) NULL,
       `k3` tinyint(4) NULL
     ) ENGINE=OLAP
     DUPLICATE KEY(`k1`, `k2`, `k3`)
     DISTRIBUTED BY HASH(`k1`) BUCKETS 5
     PROPERTIES (
       "replication_num"="1"
     );
    """

    sql """ INSERT INTO ${test_tbl} VALUES(1000, 'a', 10); """
    sql """ INSERT INTO ${test_tbl} VALUES(2000, 'b', 10); """

    // This case cannot verify the results, but it can verify abnormalities after
    // SET skip_missing_version=true
    sql """ SET skip_missing_version=true """
    qt_select_all """ select * from ${test_tbl} order by k1 """
}
