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

// This suit test the `frontends_disks` tvf
suite("test_frontends_disks_tvf") {
    List<List<Object>> table =  sql """ select * from `frontends_disks`(); """
    assertTrue(table.size() > 0)
    assertTrue(table[0].size == 10)

    // filter columns
    table = sql """ select Name from `frontends_disks`();"""
    assertTrue(table.size() > 0)
    assertTrue(table[0].size == 1)

    // case insensitive
    table = sql """ select name, host, dirtype, dir from frontends_disks() order by dirtype;"""
    assertTrue(table.size() > 0)
    assertTrue(table[0].size == 4)
    assertEquals("audit-log", table[0][2])

    // test aliase columns
    table = sql """ select name as n, host as h, dirtype as a from frontends_disks() order by dirtype; """
    assertTrue(table.size() > 0)
    assertTrue(table[0].size == 3)
    assertEquals("audit-log", table[0][2])

    // test changing position of columns
    def res = sql """ select count(*) from frontends_disks() where dirtype = 'audit-log'; """
    assertTrue(res[0][0] > 0)

    sql """ select Name, Host,
            DirType, Dir, Filesystem, Capacity, Used
            Available, UseRate, MountOn from frontends_disks();
    """

    // test exception
    test {
        sql """ select * from frontends_disks("Host" = "127.0.0.1"); """
        
        // check exception
        exception "frontends_disks table-valued-function does not support any params"
    }

}
