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

suite("test_show_frontend_master") {

    qt_sql """show frontend master;"""
    def masterResult = sql_return_maparray """show frontend master;"""
    logger.info("show frontend master result: {}", masterResult)
    if (masterResult.size() > 0) {
        def master = masterResult[0]
        def requiredColumns = ['Name', 'Host', 'EditLogPort', 'HttpPort', 'QueryPort',
                               'RpcPort', 'ArrowFlightSqlPort', 'Role', 'IsMaster',
                               'ClusterId', 'Join', 'Alive', 'ReplayedJournalId',
                               'LastStartTime', 'LastHeartbeat', 'IsHelper', 'ErrMsg',
                               'Version', 'CurrentConnected', 'LiveSince']
        def actualColumns = master.keySet()
        assertTrue("Missing required columns", actualColumns.containsAll(requiredColumns))
        assertEquals("MASTER", master.Role)
        assertEquals("true", master.IsMaster)
        assertEquals("true", master.Alive)
        assertTrue("Host should not be empty", !master.Host.isEmpty())
        assertTrue("EditLogPort should be positive", Integer.parseInt(master.EditLogPort) > 0)
        assertTrue("QueryPort should be positive", Integer.parseInt(master.QueryPort) > 0)
    }

    def allFrontends = sql_return_maparray """show frontends;"""
    def mastersFromAll = allFrontends.findAll { it.IsMaster == "true" }
    assertEquals("Master count should match", mastersFromAll.size(), masterResult.size())

    if (masterResult.size() > 0 && mastersFromAll.size() > 0) {
        def masterFromShow = masterResult[0]
        def masterFromAll = mastersFromAll[0]
        assertEquals("Host should match", masterFromShow.Host, masterFromAll.Host)
        assertEquals("EditLogPort should match", masterFromShow.EditLogPort, masterFromAll.EditLogPort)
        assertEquals("Role should match", masterFromShow.Role, masterFromAll.Role)
    }
    try {
        sql """show frontend master;"""
    } catch (Exception e) {
        logger.info("Permission test exception: {}", e.getMessage())
    }
}
