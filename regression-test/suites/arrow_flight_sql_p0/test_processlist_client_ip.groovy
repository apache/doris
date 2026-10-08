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

// An Arrow Flight SQL session used to report the placeholder 0.0.0.0:0 as its client address,
// which made SHOW PROCESSLIST, information_schema.processlist and the audit log useless for
// telling Flight sessions apart. The address is resolved when the bearer token is issued, so
// the session reports that.
suite("test_processlist_client_ip", "arrow_flight_sql") {
    // The first column marks the row of the connection running the statement.
    def processList = arrow_flight_sql """SHOW PROCESSLIST"""
    def ownRow = processList.find { "${it[0]}" == "Yes" }
    assertNotNull(ownRow, "the Flight session does not appear in its own SHOW PROCESSLIST")

    def connectionId = "${ownRow[1]}"
    def host = "${ownRow[3]}"
    logger.info("arrow flight session: id=${connectionId}, host=${host}")

    assertFalse(host.isEmpty(), "SHOW PROCESSLIST reports an empty host for the Flight session")
    assertFalse(host.startsWith("0.0.0.0"),
            "SHOW PROCESSLIST still reports the placeholder host for the Flight session: ${host}")

    // information_schema.processlist is what monitoring actually queries, and it is served from a
    // different code path (the BE schema scanner). It must show the same address. Ask over the
    // MySQL protocol, so that the address is read by a session other than the Flight one.
    //
    // The scanner asks every registered frontend for its session list, and a frontend registered
    // under more than one of its host's addresses answers once per registration, so a session can
    // come back as several identical rows. Compare the set of addresses instead of the row count:
    // every row for this session must report the address the Flight session reported above.
    def hosts = jdbc_sql """SELECT DISTINCT Host FROM information_schema.processlist WHERE Id = ${connectionId}"""
    assertEquals(1, hosts.size(),
            "information_schema.processlist does not report a single host for Flight session "
                    + "${connectionId}: ${hosts}")
    assertEquals(host, "${hosts[0][0]}")
}
