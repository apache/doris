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

suite("test_backend_selection_variables") {
    if (isCloudMode()) {
        return
    }

    def mode = sql """ show variables like 'backend_selection_mode'; """
    assertEquals("prefer", mode[0][1])
    def key = sql """ show variables like 'preferred_backend_selection_key'; """
    assertEquals("", key[0][1])
    def enable = sql """ show variables like 'enable_load_backend_selection'; """
    assertEquals("false", enable[0][1])

    try {
        sql """ set preferred_backend_selection_key = 'group_a'; """
        sql """ set backend_selection_mode = 'default'; """
        sql """ set enable_load_backend_selection = true; """

        key = sql """ show variables like 'preferred_backend_selection_key'; """
        assertEquals("group_a", key[0][1])
        mode = sql """ show variables like 'backend_selection_mode'; """
        assertEquals("default", mode[0][1])
        enable = sql """ show variables like 'enable_load_backend_selection'; """
        assertEquals("true", enable[0][1])

        def res = sql """ select 1; """
        assertEquals(1, res[0][0])

        test {
            sql """ set backend_selection_mode = 'require'; """
            exception "Backend selection provider does not support required backend selection"
        }
        test {
            sql """ set preferred_backend_selection_key = 'bad key'; """
            exception "preferred_backend_selection_key value is invalid"
        }
    } finally {
        sql """ set preferred_backend_selection_key = ''; """
        sql """ set backend_selection_mode = 'prefer'; """
        sql """ set enable_load_backend_selection = false; """
    }
}
