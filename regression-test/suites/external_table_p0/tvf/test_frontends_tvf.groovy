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

// This suit test the `frontends` tvf
suite("test_frontends_tvf","p0,external,tvf,external_docker") {
    List<List<Object>> table =  sql """ select * from `frontends`(); """
    logger.info("${table}")
    assertTrue(table.size() > 0)
    assertTrue(table[0].size() == 20)

    List<List<Object>> titleNames =  sql """ describe function frontends(); """
    assertTrue(titleNames[0][0] == "Name")
    assertTrue(titleNames[1][0] == "Host")
    assertTrue(titleNames[2][0] == "EditLogPort")
    assertTrue(titleNames[3][0] == "HttpPort")
    assertTrue(titleNames[4][0] == "QueryPort")
    assertTrue(titleNames[5][0] == "RpcPort")
    assertTrue(titleNames[6][0] == "ArrowFlightSqlPort")
    assertTrue(titleNames[7][0] == "Role")
    assertTrue(titleNames[8][0] == "IsMaster")
    assertTrue(titleNames[9][0] == "ClusterId")
    assertTrue(titleNames[10][0] == "Join")
    assertTrue(titleNames[11][0] == "Alive")
    assertTrue(titleNames[12][0] == "ReplayedJournalId")
    assertTrue(titleNames[13][0] == "LastStartTime")
    assertTrue(titleNames[14][0] == "LastHeartbeat")
    assertTrue(titleNames[15][0] == "IsHelper")
    assertTrue(titleNames[16][0] == "ErrMsg")
    assertTrue(titleNames[17][0] == "Version")
    assertTrue(titleNames[18][0] == "CurrentConnected")
    assertTrue(titleNames[19][0] == "LiveSince")

    // filter columns
    table = sql """ select Name from `frontends`();"""
    assertTrue(table.size() > 0)
    assertTrue(table[0].size() == 1)

    // case insensitive
    table = sql """ select name, host, editlogport, httpport, alive from frontends();"""
    assertTrue(table.size() > 0)
    assertTrue(table[0].size() == 5)
    assertEquals("true", table[0][4])

    // test aliase columns
    table = sql """ select name as n, host as h, alive as a, editlogport as e from frontends(); """
    assertTrue(table.size() > 0)
    assertTrue(table[0].size() == 4)
    assertEquals("true", table[0][2])

    // test changing position of columns
    def res = sql """ select count(*) from frontends() where alive = 'true'; """
    assertTrue(res[0][0] > 0)

    sql """ select Name, Host, EditLogPort
            HttpPort, QueryPort, RpcPort, ArrowFlightSqlPort, `Role`, IsMaster, ClusterId
            `Join`, Alive, ReplayedJournalId, LastHeartbeat
            IsHelper, ErrMsg, Version, CurrentConnected, LiveSince from frontends();
    """

    // test exception
    test {
        sql """ select * from frontends("Host" = "127.0.0.1"); """

        // check exception
        exception "frontends table-valued-function does not support any params"
    }
}
