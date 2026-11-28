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

// MySQL clients may run SET time_zone = 'SYSTEM' during session setup (#70809).
// SYSTEM resolves to the FE's system time zone, the same zone a new session starts with.
suite("test_set_time_zone_system") {
    def systemZone = sql("SELECT @@system_time_zone")[0][0]

    sql "SET time_zone = '+05:00'"
    sql "SET time_zone = 'SYSTEM'"
    assertEquals(systemZone, sql("SELECT @@time_zone")[0][0])

    sql "SET time_zone = '+05:00'"
    sql "SET time_zone = 'system'"
    assertEquals(systemZone, sql("SELECT @@time_zone")[0][0])

    // Time functions run with the resolved zone.
    sql "SET time_zone = 'SYSTEM'"
    def viaSystem = sql("SELECT CONVERT_TZ('2024-01-02 12:00:00', '+00:00', @@time_zone)")[0][0]
    sql "SET time_zone = '${systemZone}'"
    def viaName = sql("SELECT CONVERT_TZ('2024-01-02 12:00:00', '+00:00', @@time_zone)")[0][0]
    assertEquals(viaName, viaSystem)

    sql "SET time_zone = '+00:00'"
}
