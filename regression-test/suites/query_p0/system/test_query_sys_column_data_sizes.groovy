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

suite("test_query_sys_column_data_sizes", "query,p0") {
    def tableName = "test_column_data_sizes_table"

    qt_desc_column_data_sizes """ DESC information_schema.column_data_sizes """

    sql "DROP TABLE IF EXISTS ${tableName};"
    sql """
        CREATE TABLE ${tableName} (
            id INT NOT NULL,
            name VARCHAR(100),
            age TINYINT,
            salary DECIMAL(10, 2),
            is_active BOOLEAN,
            birth_date DATE,
            create_time DATETIME,
            score DOUBLE,
            description TEXT,
            amount BIGINT,
            tags ARRAY<VARCHAR(50)>,
            scores_map MAP<VARCHAR(20), INT>,
            person_info STRUCT<address:VARCHAR(200), phone:VARCHAR(20), email:VARCHAR(100)>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, 'Alice', 25, 50000.50, true, '1998-01-15', '2024-01-01 10:00:00', 95.5, 'First employee', 1000000,
         ['java', 'python', 'sql'], {'math':95, 'english':88}, named_struct('address', '123 Main St', 'phone', '1234567890', 'email', 'alice@example.com')),
        (2, 'Bob', 30, 60000.75, true, '1993-05-20', '2024-01-02 11:00:00', 88.0, 'Second employee', 2000000,
         ['c++', 'go'], {'math':90, 'physics':85}, named_struct('address', '456 Oak Ave', 'phone', '0987654321', 'email', 'bob@example.com')),
        (3, 'Charlie', 28, 55000.25, false, '1995-08-10', '2024-01-03 12:00:00', 92.3, 'Third employee', 1500000,
         ['javascript', 'typescript', 'react'], {'math':92, 'english':87, 'history':90}, named_struct('address', '789 Pine Rd', 'phone', '5551234567', 'email', 'charlie@example.com'))
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (4, 'David', 35, 70000.00, true, '1988-11-25', '2024-01-04 13:00:00', 90.0, 'Fourth employee with a longer description text', 3000000,
         ['rust', 'scala', 'haskell'], {'math':88, 'physics':92}, named_struct('address', '321 Elm St', 'phone', '1112223333', 'email', 'david@example.com')),
        (5, 'Eve', 27, 52000.50, false, '1996-03-12', '2024-01-05 14:00:00', 87.5, 'Fifth employee', 1800000,
         ['kotlin', 'swift'], {'math':85, 'chemistry':89}, named_struct('address', '654 Maple Dr', 'phone', '4445556666', 'email', 'eve@example.com'))
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (6, 'Frank', 32, 65000.00, true, '1991-07-08', '2024-01-06 15:00:00', 91.2, 'Sixth employee', 2500000,
         ['ruby', 'perl', 'php'], {'math':91, 'english':89}, named_struct('address', '987 Cedar Ln', 'phone', '7778889999', 'email', 'frank@example.com')),
        (7, 'Grace', 29, 58000.75, true, '1994-12-30', '2024-01-07 16:00:00', 93.8, 'Seventh employee', 2200000,
         ['r', 'matlab', 'julia'], {'math':94, 'statistics':96}, named_struct('address', '159 Birch Ct', 'phone', '2223334444', 'email', 'grace@example.com'))
    """

    sql "select * from ${tableName};"

    // Get tablet ID for the test table
    List<List<Object>> tablets = sql """ SHOW TABLETS FROM ${tableName} """
    assert tablets.size() > 0, "Should have at least one tablet"
    def tabletId = tablets[0][0]

    def result_before = sql """
        SELECT
            COLUMN_NAME,
            COLUMN_TYPE,
            COUNT(DISTINCT ROWSET_ID) as rowset_count,
            SUM(COMPRESSED_DATA_BYTES) as compressed_data_bytes,
            SUM(UNCOMPRESSED_DATA_BYTES) as uncompressed_data_bytes,
            SUM(RAW_DATA_BYTES) as raw_data_bytes
        FROM information_schema.column_data_sizes
        WHERE TABLET_ID = ${tabletId}
        GROUP BY COLUMN_NAME, COLUMN_TYPE
        ORDER BY COLUMN_NAME
    """

    logger.info("Column data sizes before compaction:")
    for (row in result_before) {
        logger.info("Column: ${row[0]}, Type: ${row[1]}, Rowset Count: ${row[2]}, Compressed Size: ${row[3]}, Uncompressed Size: ${row[4]}, Raw Data Size: ${row[5]}")
        assert row[3] > 0
        assert row[4] > 0
        assert row[5] > 0
    }

    assert result_before.size() == 13, "Should have 13 columns"

    trigger_and_wait_compaction(tableName, "full")

    def result_after = sql """
        SELECT
            COLUMN_NAME,
            COLUMN_TYPE,
            COUNT(DISTINCT ROWSET_ID) as rowset_count,
            SUM(COMPRESSED_DATA_BYTES) as compressed_data_bytes,
            SUM(UNCOMPRESSED_DATA_BYTES) as uncompressed_data_bytes,
            SUM(RAW_DATA_BYTES) as raw_data_bytes
        FROM information_schema.column_data_sizes
        WHERE TABLET_ID = ${tabletId}
        GROUP BY COLUMN_NAME, COLUMN_TYPE
        ORDER BY COLUMN_NAME
    """

    logger.info("Column data sizes after compaction:")
    for (row in result_after) {
        logger.info("Column: ${row[0]}, Type: ${row[1]}, Rowset Count: ${row[2]}, Compressed Size: ${row[3]}, Uncompressed Size: ${row[4]}, Raw Data Size: ${row[5]}")
        assert row[3] > 0
        assert row[4] > 0
        assert row[5] > 0
    }

    assert result_after.size() == 13, "Should still have 13 columns after compaction"
}
