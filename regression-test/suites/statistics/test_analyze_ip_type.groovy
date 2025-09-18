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

suite("test_analyze_ip_type") {

    sql """drop database if exists test_analyze_ip_type"""
    sql """create database test_analyze_ip_type"""
    sql """use test_analyze_ip_type"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""
    def tableName = "iptest"
    sql """drop table if exists ${tableName}"""
    sql """ 
        CREATE TABLE ${tableName} (
          `id` bigint,
          `ip_v4` ipv4,
          `ip_v6` ipv6
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );  
    """
    sql """insert into ${tableName} values(-1, NULL, NULL)"""
    sql """insert into ${tableName} values(0, '0.0.0.0', '::')"""
    sql """insert into ${tableName} values(1, '0.0.0.1', '::1')"""
    sql """insert into ${tableName} values(2130706433, '127.0.0.1', '2001:1b70:a1:610::b102:2')"""
    sql """insert into ${tableName} values(4294967295, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')"""
    sql """analyze table ${tableName} with sync"""

    def result = sql """show column stats ${tableName}"""
    assertEquals(3, result.size())
    result = sql """show column cached stats ${tableName}"""
    assertEquals(3, result.size())

    result = sql """show column stats ${tableName} (ip_v6);"""
    assertEquals("ip_v6", result[0][0])
    assertEquals("iptest", result[0][1])
    assertEquals("5.0", result[0][2])
    assertEquals("4.0", result[0][3])
    assertEquals("1.0", result[0][4])
    assertEquals("80.0", result[0][5])
    assertEquals("16.0", result[0][6])
    assertEquals("\"::\"", result[0][7])
    assertEquals("\"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff\"", result[0][8])

    result = sql """show column cached stats ${tableName} (ip_v6);"""
    assertEquals("ip_v6", result[0][0])
    assertEquals("iptest", result[0][1])
    assertEquals("5.0", result[0][2])
    assertEquals("4.0", result[0][3])
    assertEquals("1.0", result[0][4])
    assertEquals("80.0", result[0][5])
    assertEquals("16.0", result[0][6])
    assertEquals("\"::\"", result[0][7])
    assertEquals("\"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff\"", result[0][8])

    result = sql """show column stats ${tableName} (ip_v4);"""
    assertEquals("ip_v4", result[0][0])
    assertEquals("iptest", result[0][1])
    assertEquals("5.0", result[0][2])
    assertEquals("4.0", result[0][3])
    assertEquals("1.0", result[0][4])
    assertEquals("20.0", result[0][5])
    assertEquals("4.0", result[0][6])
    assertEquals("\"0.0.0.0\"", result[0][7])
    assertEquals("\"255.255.255.255\"", result[0][8])

    result = sql """show column cached stats ${tableName} (ip_v4);"""
    assertEquals("ip_v4", result[0][0])
    assertEquals("iptest", result[0][1])
    assertEquals("5.0", result[0][2])
    assertEquals("4.0", result[0][3])
    assertEquals("1.0", result[0][4])
    assertEquals("20.0", result[0][5])
    assertEquals("4.0", result[0][6])
    assertEquals("\"0.0.0.0\"", result[0][7])
    assertEquals("\"255.255.255.255\"", result[0][8])

    sql """drop database if exists test_analyze_ip_type"""
}


