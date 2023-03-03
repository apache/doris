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

// There once exists a bug that when passing a too long string
// along with an empty one to find_in_set, BE 

suite("test_not_supported_agg_fn") {

    try {
        test {
            sql """
            SELECT ref_0.`PS_SUPPKEY` AS c0,
            ref_0.`PS_SUPPLYCOST` AS c1,
            last_value(cast(bitmap_empty() AS BITMAP)) OVER (PARTITION BY ref_0.`PS_AVAILQTY`
                                                        ORDER BY ref_0.`PS_PARTKEY` DESC) AS c2,
                                                       CASE
                                                           WHEN FALSE THEN ref_0.`PS_COMMENT`
                                                           ELSE ref_0.`PS_COMMENT`
                                                       END AS c3
            FROM regression_test_tpch_sf0_1_p1.partsupp AS ref_0
            WHERE ref_0.`PS_PARTKEY` IS NULL
            ORDER BY ref_0.`PS_COMMENT`
            """
            exception "errCode = 2, detailMessage = No matching function with signature: last_value(bitmap)"            
        }
    } finally {
    }

}