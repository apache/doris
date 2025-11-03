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

suite("test_other_conjuncts_nullable") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """DROP TABLE IF EXISTS other_conjuncts_t0"""
    sql """DROP TABLE IF EXISTS other_conjuncts_t1"""

    sql """ CREATE TABLE other_conjuncts_t0(c0 CHAR(94)) DISTRIBUTED BY HASH (c0) PROPERTIES ("replication_num" = "1");"""
    sql """ CREATE TABLE other_conjuncts_t1(c1 CHAR(94) NOT NULL) DUPLICATE KEY(c1) DISTRIBUTED BY RANDOM BUCKETS 16 PROPERTIES ("replication_num" = "1");"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('ACBy');"""
    sql """ INSERT INTO other_conjuncts_t1 (c1) VALUES ('QMOpD贷_Td');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('贷\');"""
    sql """ INSERT INTO other_conjuncts_t1 (c1) VALUES ('aWZ*'), ('1d!贷*u{ye'), ('c');"""
    sql """ INSERT INTO other_conjuncts_t1 (c1) VALUES ('\\'K\\r');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('C7t2u!4');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('k_䳜doj>.W');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('KX');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('YA');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('k_䳜doj>.W');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('');"""
    sql """ INSERT INTO other_conjuncts_t1 (c1) VALUES ('');"""
    sql """ INSERT INTO other_conjuncts_t1 (c1) VALUES ('');"""
    sql """ INSERT INTO other_conjuncts_t1 (c1) VALUES ('-1096708809');"""
    sql """ INSERT INTO other_conjuncts_t1 (c1) VALUES ('');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('4,szzM䳜wz'), (''), ('uU\nXtdP+V');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('隺E');"""
    sql """ INSERT INTO other_conjuncts_t0 (c0) VALUES ('');"""
    sql """ INSERT INTO other_conjuncts_t1 (c1) VALUES ('');"""

    sql """sync"""

    sql """
        SELECT
            1
        FROM
            other_conjuncts_t0
            LEFT OUTER JOIN other_conjuncts_t1 ON (
                true NOT IN (
                    true,
                    CASE
                        ((other_conjuncts_t0.c0) LIKE ('12235502'))
                        WHEN (NOT false) THEN ((other_conjuncts_t1.c1) like (''))
                    END
                )
            );
    """
}
