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

suite("test_string_literal_vs_other_literal") {
    qt_numeric_eq """select 1 where "12345678901234567890" = 12345678901234567890;"""
    qt_numeric_non_eq """select 1 where "12345678901234567890" = 12345678901234567891;"""

    qt_date_eq """select 1 where "2024-06-24 00:01:02.345678" = cast("2024-06-24 00:01:02.345678" as datetime(6)) """
    qt_date_eq """select 1 where "2024-06-24 00:01:02.345679" = cast("2024-06-24 00:01:02.345678" as datetime(6)) """
}
