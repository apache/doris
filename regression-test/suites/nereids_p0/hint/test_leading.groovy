/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("test_leading") {
    // create database and tables
    sql 'DROP DATABASE IF EXISTS test_leading'
    sql 'CREATE DATABASE IF NOT EXISTS test_leading'
    sql 'use test_leading'
    sql """CREATE TABLE IF NOT EXISTS t1 (l1 INT) DUPLICATE KEY (l1) DISTRIBUTED BY HASH (l1) PROPERTIES('replication_num' = '1');"""
    sql """CREATE TABLE IF NOT EXISTS t2 (l2 INT) DUPLICATE KEY (l2) DISTRIBUTED BY HASH (l2) PROPERTIES('replication_num' = '1');"""

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    // basic usage
    qt_select """ explain shape plan select /*+ leading(t2, t1) */ * from t1 join t2 on l1 = l2; """
}
