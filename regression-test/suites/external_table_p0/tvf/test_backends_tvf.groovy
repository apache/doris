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
suite("test_backends_tvf","p0,external,tvf,external_docker") {
    List<List<Object>> table =  sql """ select * from backends(); """
    assertTrue(table.size() > 0)
    assertEquals(29, table[0].size())

    List<List<Object>> titleNames = sql """ describe function backends(); """
    assertTrue(titleNames[0][0] =="BackendId")
    assertTrue(titleNames[1][0] =="Host")
    assertTrue(titleNames[2][0] =="HeartbeatPort")
    assertTrue(titleNames[3][0] =="BePort")
    assertTrue(titleNames[4][0] =="HttpPort")
    assertTrue(titleNames[5][0] =="BrpcPort")
    assertTrue(titleNames[6][0] =="ArrowFlightSqlPort")
    assertTrue(titleNames[7][0] =="LastStartTime")
    assertTrue(titleNames[8][0] =="LastHeartbeat")
    assertTrue(titleNames[9][0] =="Alive")
    assertTrue(titleNames[10][0] =="SystemDecommissioned")
    assertTrue(titleNames[11][0] =="TabletNum")
    assertTrue(titleNames[12][0] =="DataUsedCapacity")
    assertTrue(titleNames[13][0] =="TrashUsedCapacity")
    assertTrue(titleNames[14][0] =="AvailCapacity")
    assertTrue(titleNames[15][0] =="TotalCapacity")
    assertTrue(titleNames[16][0] =="UsedPct")
    assertTrue(titleNames[17][0] =="MaxDiskUsedPct")
    assertTrue(titleNames[18][0] =="RemoteUsedCapacity")
    assertTrue(titleNames[19][0] =="Tag")
    assertTrue(titleNames[20][0] =="ErrMsg")
    assertTrue(titleNames[21][0] =="Version")
    assertTrue(titleNames[22][0] =="Status")
    assertTrue(titleNames[23][0] =="HeartbeatFailureCounter")
    assertTrue(titleNames[24][0] =="CpuCores")
    assertTrue(titleNames[25][0] =="Memory")
    assertTrue(titleNames[26][0] =="LiveSince")
    assertTrue(titleNames[27][0] =="RunningTasks")
    assertTrue(titleNames[28][0] =="NodeRole")

    // filter columns
    table = sql """ select BackendId, Host, Alive, TotalCapacity, Version, NodeRole from backends();"""
    assertTrue(table.size() > 0)
    assertTrue(table[0].size() == 6)
    assertEquals(true, table[0][2])

    // case insensitive
    table = sql """ select backendid, Host, alive, Totalcapacity, version, nodeRole from backends();"""
    assertTrue(table.size() > 0)
    assertTrue(table[0].size() == 6)
    assertEquals(true, table[0][2])

    // test aliase columns
    table = sql """ select backendid as id, Host as name, alive, NodeRole as r from backends();"""
    assertTrue(table.size() > 0)
    assertTrue(table[0].size() == 4)
    assertEquals(true, table[0][2])

    // test changing position of columns
    table = sql """ select Host as name, NodeRole as r, alive from backends();"""
    assertTrue(table.size() > 0)
    assertTrue(table[0].size() == 3)
    assertEquals(true, table[0][2])

    def res = sql """ select count(*) from backends() where alive = 1; """
    assertTrue(res[0][0] > 0)

    res = sql """ select count(*) from backends() where alive = true; """
    assertTrue(res[0][0] > 0)

    sql """ select BackendId, Host, HeartbeatPort,
            BePort, HttpPort, BrpcPort, LastStartTime, LastHeartbeat, Alive
            SystemDecommissioned, tabletnum
            DataUsedCapacity, AvailCapacity, TotalCapacity, UsedPct
            MaxDiskUsedPct, RemoteUsedCapacity, Tag, ErrMsg, Version, Status
            HeartbeatFailureCounter, CpuCores, Memory, LiveSince, RunningTasks, NodeRole from backends();
    """


    // test exception
    test {
        sql """ select * from backends("backendId" = "10003"); """

        // check exception
        exception "backends table-valued-function does not support any params"
    }
}
