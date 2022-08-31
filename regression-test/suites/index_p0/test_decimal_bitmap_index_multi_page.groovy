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
suite("test_decimal_bitmap_index_multi_page") {
    def tbName = "test_decimal_bitmap_index_multi_page"

    qt_sql "desc ${tbName};"
    qt_sql "SHOW INDEX FROM ${tbName};"
    qt_sql "select * from ${tbName} order by a ASC limit 3;"
    qt_sql "select * from ${tbName} order by a DESC limit 3;"

}
