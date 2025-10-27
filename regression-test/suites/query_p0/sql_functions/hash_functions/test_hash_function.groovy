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
suite("test_hash_function", "arrow_flight_sql") {
    sql "set batch_size = 4096;"
    sql "set enable_profile = true;"

    qt_sql "SELECT murmur_hash3_32(null);"
    qt_sql "SELECT murmur_hash3_32(\"hello\");"
    qt_sql "SELECT murmur_hash3_32(\"hello\", \"world\");"

    qt_sql "SELECT murmur_hash3_64(null);"
    qt_sql "SELECT murmur_hash3_64(\"hello\");"
    qt_sql "SELECT murmur_hash3_64(\"hello\", \"world\");"

    // Keep the results same with `mmh3.hash64` in python or `murmur3.Sum64` in go
    // Please dont auto genOut for this test
    qt_mmh3_64_v2_1 "SELECT MURMUR_HASH3_64_V2(NULL);"
    qt_mmh3_64_v2_2 "SELECT MURMUR_HASH3_64_V2('1000209601_1756808272');"
    qt_mmh3_64_v2_3 "SELECT MURMUR_HASH3_64_V2('hello world');"
    qt_mmh3_64_v2_4 "SELECT MURMUR_HASH3_64_V2('apache doris');"

    qt_sql "SELECT xxhash_32(null);"
    qt_sql "SELECT xxhash_32(\"hello\");"
    qt_sql "SELECT xxhash_32(\"hello\", \"world\");"

    qt_sql "SELECT xxhash_64(null);"
    qt_sql "SELECT xxhash_64(\"hello\");"
    qt_sql "SELECT xxhash_64(\"hello\", \"world\");"

    def xxhash_res = sql "SELECT xxhash_64(null);"
    def xxhash3_res = sql "SELECT xxhash3_64(null);"
    assertEquals(xxhash_res, xxhash3_res);

    xxhash_res = sql "SELECT xxhash_64(\"hello\");"
    xxhash3_res = sql "SELECT xxhash3_64(\"hello\");"
    assertEquals(xxhash_res, xxhash3_res);

    xxhash_res = sql "SELECT xxhash_64(\"hello\", \"world\");"
    xxhash3_res = sql "SELECT xxhash3_64(\"hello\", \"world\");"
    assertEquals(xxhash_res, xxhash3_res);
}
