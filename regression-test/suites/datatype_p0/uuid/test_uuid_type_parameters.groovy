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

// Checklist: A02 B01 C12.
suite("test_uuid_type_parameters", "p0") {
    for (String type : ["UUID(16)", "UUID(16,0)", "ARRAY<UUID(36)>", "MAP<UUID(16),UUID>"]) {
        test {
            sql "CREATE TABLE uuid_invalid_type (id INT, u ${type}) DISTRIBUTED BY HASH(id) PROPERTIES('replication_num'='1')"
            exception "UUID does not support length or precision parameters"
        }
        test {
            sql "SELECT CAST(NULL AS ${type})"
            exception "UUID does not support length or precision parameters"
        }
    }
}
