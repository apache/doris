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

suite("create_table_like_p0") {
    sql """DROP TABLE IF EXISTS test_create_table_like"""
    sql """ 
            CREATE TABLE test_create_table_like (
              a DATEV2 NOT NULL COMMENT "'a' is a date, but it's not a date/n,/r/n/t/n/'",
              b VARCHAR(96) NOT NULL COMMENT ' /"a/" is a date, /n/,/r/n/t/n',
              c VARCHAR(96) NOT NULL COMMENT 'c',
              d VARCHAR(96) COMMENT '',
              e bigint NOT NULL  )
            DISTRIBUTED BY HASH(e) BUCKETS 1
            PROPERTIES( 'replication_num' = '1');
    """
    sql """DROP TABLE IF EXISTS tctl_2"""
    sql """DROP TABLE IF EXISTS tctl_3"""
    sql """DROP TABLE IF EXISTS tctl_1"""
    sql """create table tctl_1 like test_create_table_like"""
    sql """create table tctl_2 like tctl_1"""
    sql """create table tctl_3 like tctl_2"""
}