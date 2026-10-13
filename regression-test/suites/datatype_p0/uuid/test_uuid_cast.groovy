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

// Checklist: A03 A06 C01 E02 G01 G05 H01 H02 H03.
suite("test_uuid_cast", "p0") {
    // String/CHAR/VARCHAR/VARIANT conversions, invalid boundaries and nested values.
    order_qt_uuid_casts """
        SELECT
            CAST(CAST('550E8400E29B41D4A716446655440000' AS CHAR(32)) AS UUID),
            CAST(CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS VARCHAR(36)) AS UUID),
            CAST(CAST('00000000-0000-0000-0000-000000000001' AS UUID) AS STRING),
            CAST('00000000-0000-0000-0000-00000000001' AS UUID),
            CAST('00000000-0000-0000-0000-0000000000000' AS UUID),
            CAST(CAST('550e8400-e29b-41d4-a716-446655440000' AS VARIANT) AS UUID)
    """
    test {
        sql "SELECT CAST(1 AS UUID)"
        exception "cannot cast"
    }
    order_qt_uuid_nested_types """
        SELECT
            CAST(ARRAY(CAST('00000000-0000-0000-0000-000000000001' AS UUID),
                       CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID)) AS STRING),
            CAST(MAP(CAST('00000000-0000-0000-0000-000000000001' AS UUID), 'one') AS STRING),
            CAST(NAMED_STRUCT('u',
                CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS UUID)) AS STRING),
            TO_JSON(CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID))
    """
}
