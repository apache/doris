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

suite("test_json_valid_strict") {
    def jsonValid = sql """
        select
            json_valid('not json'),
            json_valid('not'),
            json_valid('null junk'),
            json_valid('{"a":1} junk'),
            json_valid('{"a":1}')
        from (select 1) t
        order by 1
    """
    assertEquals(0, jsonValid[0][0])
    assertEquals(0, jsonValid[0][1])
    assertEquals(0, jsonValid[0][2])
    assertEquals(0, jsonValid[0][3])
    assertEquals(1, jsonValid[0][4])
}
