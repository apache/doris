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
suite("test_hash_function") {
    sql "set enable_vectorized_engine = true;"
    sql "set batch_size = 4096;"

    qt_sql "SELECT murmur_hash3_32(null);"
    qt_sql "SELECT murmur_hash3_32(\"hello\");"
    qt_sql "SELECT murmur_hash3_32(\"hello\", \"world\");"

    qt_sql "SELECT murmur_hash3_64(null);"
    qt_sql "SELECT murmur_hash3_64(\"hello\");"
    qt_sql "SELECT murmur_hash3_64(\"hello\", \"world\");"
}
