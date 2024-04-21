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

suite("test_show_data_skew") {
    sql """
        CREATE TABLE test_show_data_skew (
            id int, 
            name string, 
            pdate DATETIME) 
        PARTITION BY RANGE(pdate) ( 
            FROM ("2023-04-16") TO ("2023-04-20") INTERVAL 1 DAY 
        ) DISTRIBUTED BY HASH(id) BUCKETS 5 
        properties("replication_num" = "1");
        """
    def result = sql """show data skew from test_show_data_skew;"""
    assertTrue(result.size() == 20)

    def result2 = sql """show data skew from test_show_data_skew partition(p_20230416);"""
    assertTrue(result2.size() == 5)

    def result3 = sql """show data skew from test_show_data_skew partition(p_20230416, p_20230418);"""
    assertTrue(result3.size() == 10)
}
