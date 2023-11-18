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

suite("test_digest") {
    qt_md5 "select md5(k6) from test_query_db.test order by k6"
    qt_sha1_1 "select sha1(\"123\")"
    qt_sha1_2 "select sha(k7), sha1(k7) from test_query_db.test order by k7"
    qt_sha1_3 "select sha1(\"\")"
    qt_sha1_4 "select sha1(NULL)"
    qt_sha2_1 "select sha2(k7, 256) from test_query_db.test order by k7"
    qt_sha2_2 "select sha2(k7, 512) from test_query_db.test order by k7"
    qt_sha2_3 "select sha2('abc', 224)"
    qt_sha2_4 "select sha2('abc', 384)"
    qt_sha2_5 "select sha2(NULL, 384)"

    try {
        result = sql """ select sha2("123", 255) """
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("only support 224/256/384/512"))
    }
}
