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

suite("test_st_isclosed") {
    def result = sql """
        SELECT ST_IsClosed(ST_LineFromText('LINESTRING (0 0, 1 1, 0 0)')),
               ST_IsClosed(ST_LineFromText('LINESTRING (0 0, 1 1, 2 2)')),
               ST_IsClosed(NULL),
               ST_IsClosed(ST_Point(0, 0))
    """

    assertEquals(true, result[0][0])
    assertEquals(false, result[0][1])
    assertEquals(null, result[0][2])
    assertEquals(null, result[0][3])
}
