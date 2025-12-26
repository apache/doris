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

suite("array_flatten") {
    sql """DROP TABLE IF EXISTS t_array_flatten"""
    sql """
            CREATE TABLE IF NOT EXISTS t_array_flatten (
              `k1` int(11) NULL COMMENT "",
              `a1` array<tinyint(4)> NULL COMMENT "",
              `aaa1` array<array<array<tinyint(4)>>> NULL COMMENT "",
              `aa3` array<array<int(11)>> NOT NULL COMMENT "",
              `aa5` array<array<largeint(40)>> NULL COMMENT "",
              `aa14` array<array<string>> NULL COMMENT ""

            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO t_array_flatten VALUES(1, [1, 2, 3],[[[1]],[[2],[3]]],[[1,2],[3]],[[1,2],[3]],[['a'],['b','c']]) """
    sql """ INSERT INTO t_array_flatten VALUES(2, null,null,[],null,[null,['b',null]]) """
    sql """ INSERT INTO t_array_flatten VALUES(3, [1, 2, null],[[[]],[[null],[]]],[[null,2],[]],[[null,null],[3]],[[null],['aaaab','ccc']]) """



    qt_test """
    select k1, array_flatten(a1), array_flatten(aaa1), array_flatten(aa3), array_flatten(aa5), array_flatten(aa14) from t_array_flatten order by k1;
    """
}
