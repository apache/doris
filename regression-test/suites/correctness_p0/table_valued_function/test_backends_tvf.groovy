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

// This suit test the `backends` tvf
suite("test_backends_tvf") {
    List<List<Object>> table =  sql """ select * from backends(); """
    assertTrue(table.size() > 0) // row should > 0
    assertTrue(table[0].size == 26) // column should be 26

    // filter columns
    table = sql """ select BackendId, HostName, Alive, TotalCapacity, Version, NodeRole from backends();"""
    assertTrue(table.size() > 0) // row should > 0
    assertTrue(table[0].size == 6) // column should be 26
    assertEquals("true", table[0][2])

    // case insensitive
    table = sql """ select backendid, Hostname, alive, Totalcapacity, version, nodeRole from backends();"""
    assertTrue(table.size() > 0) // row should > 0
    assertTrue(table[0].size == 6) // column should be 26
    assertEquals("true", table[0][2])

    // test aliase columns
    table = sql """ select backendid as id, Hostname as name, alive, NodeRole as r from backends();"""
    assertTrue(table.size() > 0) // row should > 0
    assertTrue(table[0].size == 4) // column should be 26
    assertEquals("true", table[0][2])

    // test changing position of columns
    table = sql """ select Hostname as name, NodeRole as r, alive, ip from backends();"""
    assertTrue(table.size() > 0) // row should > 0
    assertTrue(table[0].size == 4) // column should be 26
    assertEquals("true", table[0][2])


}