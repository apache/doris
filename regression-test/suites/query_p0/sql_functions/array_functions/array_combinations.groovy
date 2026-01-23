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

suite("array_combinations") {
    sql """DROP TABLE IF EXISTS t_array_combinations"""
    sql """
            CREATE TABLE IF NOT EXISTS t_array_combinations (
              `k1` int(11) NULL COMMENT "",
              `s1` array<string> NULL COMMENT "",
              `a1` array<tinyint(4)> NULL COMMENT "",
              `a2` array<largeint(40)> NULL COMMENT "",
              `aa1` array<array<int(11)>> NOT NULL COMMENT "",
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO t_array_combinations VALUES(1, ['foo','bar','baz'], [1,2,3], [1,2,2], [[1,1],[4,5],[1,4]]) """

    qt_test """
    select k1, array_combinations(s1, 2), array_combinations(a1, 2), array_combinations(a2, 2), array_combinations(aa1,  2) from t_array_combinations order by k1;
    """
}