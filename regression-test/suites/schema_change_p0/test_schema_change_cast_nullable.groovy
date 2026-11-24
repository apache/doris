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

suite("test_schema_change_cast_nullable", "p0") {
    sql "DROP TABLE IF EXISTS schema_change_cast_nullable"
    sql """
        CREATE TABLE schema_change_cast_nullable (
            id INT NOT NULL,
            required_dt DATETIMEV2(3) NOT NULL,
            nullable_dt DATETIMEV2(3) NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "light_schema_change" = "false")
    """
    sql """
        INSERT INTO schema_change_cast_nullable VALUES
        (1, '2024-02-29 12:34:56.123', '2024-02-29 12:34:56.123'),
        (2, '9999-12-31 23:59:59.999', NULL),
        (3, '2000-01-01 00:00:00.000', '2000-01-01 00:00:00.001')
    """
    qt_before "SELECT * FROM schema_change_cast_nullable ORDER BY id"
    sql """
        ALTER TABLE schema_change_cast_nullable
        MODIFY COLUMN required_dt DATETIMEV2(6) NOT NULL,
        MODIFY COLUMN nullable_dt DATETIMEV2(6) NULL
    """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='schema_change_cast_nullable'
               ORDER BY JobId DESC LIMIT 1"""
        time 120
    }
    qt_after "SELECT * FROM schema_change_cast_nullable ORDER BY id"
    // Exercise writes using the new schema as well as the rewritten historical rowsets.
    sql """
        INSERT INTO schema_change_cast_nullable VALUES
        (4, '2024-02-29 12:34:56.123456', '2024-02-29 12:34:56.654321')
    """
    qt_insert_after "SELECT * FROM schema_change_cast_nullable ORDER BY id"
    qt_expected_downscale """
        SELECT id, CAST(required_dt AS DATETIMEV2(3)), CAST(nullable_dt AS DATETIMEV2(3))
        FROM schema_change_cast_nullable ORDER BY id
    """
    sql """
        ALTER TABLE schema_change_cast_nullable
        MODIFY COLUMN required_dt DATETIMEV2(3) NOT NULL,
        MODIFY COLUMN nullable_dt DATETIMEV2(3) NULL
    """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='schema_change_cast_nullable'
               ORDER BY JobId DESC LIMIT 1"""
        time 120
    }
    qt_after_downscale "SELECT * FROM schema_change_cast_nullable ORDER BY id"
}
