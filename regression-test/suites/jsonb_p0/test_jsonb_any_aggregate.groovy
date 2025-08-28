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

suite("test_jsonb_any_aggregate", "p0") {
    def tableName = "test_any_json"

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
        id INT,
        group_id INT,
        json_data JSON
    )
    DISTRIBUTED BY HASH(id) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    sql """
    INSERT INTO ${tableName} VALUES
    (1, 1, '{"name": "Alice", "age": 25}'),
    (2, 2, '{"name": "Bob", "age": 30}'),
    (3, 3, '{"name": "Charlie", "age": 35}')
    """

    qt_sql """
    SELECT group_id, ANY(json_data) as any_data
    FROM ${tableName}
    GROUP BY group_id
    ORDER BY group_id
    """

    qt_sql """
    SELECT group_id, ANY_VALUE(json_data) as any_value_data
    FROM ${tableName}
    GROUP BY group_id
    ORDER BY group_id
    """

    qt_sql """
    WITH t0 AS (
      SELECT CAST(1 AS BIGINT) AS id, CAST('{}' AS JSON) AS attr
    )
    SELECT id, ANY(attr) FROM t0 GROUP BY id
    """

    qt_sql """
    SELECT ANY(CAST('{}' AS JSON))
    """

    qt_sql """
    SELECT ANY_VALUE(CAST('{}' AS JSON))
    """

    sql "TRUNCATE TABLE ${tableName}"

    sql """
    INSERT INTO ${tableName} VALUES
    (1, 1, '{"info": {"address": {"city": "Beijing", "zipcode": 100000}, "contacts": [{"type": "phone", "value": "123456"}, {"type": "email", "value": "test@example.com"}]}}')
    """

    qt_sql """
    SELECT ANY(json_data) FROM ${tableName} WHERE id = 1
    """

    sql "TRUNCATE TABLE ${tableName}"
    sql """
    INSERT INTO ${tableName} VALUES
    (1, 1, '{"value": 100}'),
    (2, 1, '{"value": 100}'),
    (3, 1, '{"value": 100}')
    """

    qt_sql """
    SELECT group_id, ANY(json_data) as any_data
    FROM ${tableName}
    GROUP BY group_id
    """

    // testing for null values
    sql "TRUNCATE TABLE ${tableName}"
    sql """
    INSERT INTO ${tableName}(id, group_id) VALUES
    (1, 1),
    (2, 1)
    """

    qt_sql """
    SELECT group_id, ANY(json_data) as any_data
    FROM ${tableName}
    GROUP BY group_id
    """
}