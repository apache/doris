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

suite("test_show_queued_analyze_jobs", "nonConcurrent") {
    sql """drop database if exists test_show_queued_analyze_jobs"""
    sql """create database test_show_queued_analyze_jobs"""
    sql """use test_show_queued_analyze_jobs"""
    sql """drop table if exists table1"""
    sql """
        CREATE TABLE IF NOT EXISTS table1  (
            col1 integer not null,
            col2 integer not null,
            col3 integer not null
        )
        DUPLICATE KEY(col1)
        DISTRIBUTED BY HASH(col1) BUCKETS 2
        PROPERTIES ("replication_num" = "1");
    """
    sql """insert into table1 values (1, 1, 1), (2, 2, 2)"""

    try {
        sql """set global enable_auto_analyze=true"""
        sql """select col2 from table1 where col1 > 0"""
        def result;
        for (int i = 0; i < 10; i++) {
            result = sql """show queued analyze jobs"""
            if (result.size() == 0) {
                logger.info("job not queued yet.")
                Thread.sleep(1000)
            } else {
                break;
            }
        }
        logger.info("show queued analyze job result:" + result)
    } finally {
        sql """set global enable_auto_analyze=false"""
    }
}

