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

suite("test_timestamptz_lambda_array_sort") {
    sql "set time_zone = '+08:00';"

    qt_lambda_array_sort """
        SELECT array_sort(
            (`x`, `y`) -> CAST(0 AS TINYINT),
            ARRAY(CAST('2026-08-18 04:34:56 +00:00' AS TIMESTAMPTZ(6)))
        );
    """
}
