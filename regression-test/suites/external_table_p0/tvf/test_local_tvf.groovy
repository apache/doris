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
suite("test_local_tvf") {
    List<List<Object>> table =  sql """ select * from backends(); """
    assertTrue(table.size() > 0)

    if ( table.size() == 1) {
        // here may do not have fe log with multi doris be cluster
        def be_id = table[0][0]

        List<List<Object>> doris_log = sql """ ADMIN SHOW FRONTEND CONFIG like "sys_log_dir"; """
        assertTrue(doris_log.size() > 0)
        def doris_log_path = doris_log[0][1]

        table = sql """
            select count(*) from local(
                "file_path" = "${doris_log_path}/fe.out",
                "backend_id" = "${be_id}",
                "format" = "csv")
            where c1 like "%FE type%";"""

        assertTrue(table.size() > 0)
        assertTrue(Long.valueOf(table[0][0]) > 0)

        table = sql """
            select count(*) from local(
                "file_path" = "${doris_log_path}/*.out",
                "backend_id" = "${be_id}",
                "format" = "csv")
            where c1 like "%FE type%";"""

        assertTrue(table.size() > 0)
        assertTrue(Long.valueOf(table[0][0]) > 0)

        test {
            sql """
            select count(*) from local(
                "file_path" = "../fe.out",
                "backend_id" = "${be_id}",
                "format" = "csv")
            where c1 like "%FE type%";
            """
            // check exception message contains
            exception "can not contain '..' in path"
        }

        test {
            sql """
            select count(*) from local(
                "file_path" = "./xx.out",
                "backend_id" = "${be_id}",
                "format" = "csv")
            where c1 like "%FE type%";
            """
            // check exception message contains
            exception "No matches found"
        }
    }
}
