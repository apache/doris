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

suite("test_fileset_table") {
    // Drop before test to preserve environment for debugging
    sql "DROP TABLE IF EXISTS test_fileset_tbl"
    sql "DROP TABLE IF EXISTS test_fileset_tbl2"

    // Basic CREATE TABLE with ENGINE=fileset using S3 location
    sql """
        CREATE TABLE test_fileset_tbl (
            `file` FILE NULL
        )
        ENGINE = fileset
        PROPERTIES (
            'location' = 's3://test-bucket/data/parquet/*',
            's3.region' = 'us-east-1',
            's3.endpoint' = 'https://s3.us-east-1.amazonaws.com',
            's3.access_key' = 'test_ak',
            's3.secret_key' = 'test_sk'
        )
    """

    // Table should appear in SHOW TABLES
    def tables = sql "SHOW TABLES LIKE 'test_fileset_tbl'"
    assertTrue(tables.size() > 0)

    // SHOW CREATE TABLE should reflect engine and location
    def ddl = sql "SHOW CREATE TABLE test_fileset_tbl"
    assertTrue(ddl[0][1].contains("fileset"))
    assertTrue(ddl[0][1].contains("s3://test-bucket/data/parquet/*"))

    // DESC should show a single FILE column
    def desc = sql "DESC test_fileset_tbl"
    assertEquals(1, desc.size())
    assertEquals("file", desc[0][0])
    assertTrue(desc[0][1].toLowerCase().contains("file"))

    // CREATE TABLE with wildcard-free path should also succeed
    sql """
        CREATE TABLE test_fileset_tbl2 (
            `file` FILE NULL
        )
        ENGINE = fileset
        PROPERTIES (
            'location' = 's3://test-bucket/data/parquet/*.jpg',
            's3.region' = 'us-east-1',
            's3.endpoint' = 'https://s3.us-east-1.amazonaws.com',
            's3.access_key' = 'test_ak',
            's3.secret_key' = 'test_sk'
        )
    """

    // Missing location should fail
    test {
        sql """
            CREATE TABLE test_fileset_bad (
                `file` FILE NULL
            )
            ENGINE = fileset
            PROPERTIES (
                's3.region' = 'us-east-1'
            )
        """
        exception "location"
    }

    // Wrong column type should fail
    test {
        sql """
            CREATE TABLE test_fileset_bad2 (
                `id` INT NOT NULL
            )
            ENGINE = fileset
            PROPERTIES (
                'location' = 's3://test-bucket/data/',
                's3.region' = 'us-east-1'
            )
        """
        exception "FILE"
    }

    // Multiple columns should fail
    test {
        sql """
            CREATE TABLE test_fileset_bad3 (
                `file` FILE NULL,
                `id` INT NOT NULL
            )
            ENGINE = fileset
            PROPERTIES (
                'location' = 's3://test-bucket/data/',
                's3.region' = 'us-east-1'
            )
        """
        exception "FILE"
    }

    // ============ Query Restriction Tests ============
    // FILE type must not be used in ORDER BY, GROUP BY, JOIN, etc.
    // These checks happen at FE analysis, no S3 access needed.

    // ORDER BY on FILE column
    test {
        sql "SELECT `file` FROM test_fileset_tbl ORDER BY `file`"
        exception "don't support"
    }

    // ORDER BY with LIMIT (LogicalTopN path)
    test {
        sql "SELECT `file` FROM test_fileset_tbl ORDER BY `file` LIMIT 10"
        exception "don't support"
    }

    // GROUP BY on FILE column
    test {
        sql "SELECT `file` FROM test_fileset_tbl GROUP BY `file`"
        exception "don't support"
    }

    // DISTINCT on FILE column (rewrites to GROUP BY)
    test {
        sql "SELECT DISTINCT `file` FROM test_fileset_tbl"
        exception "don't support"
    }

    // MIN aggregate on FILE column
    test {
        sql "SELECT MIN(`file`) FROM test_fileset_tbl"
        exception "don't support"
    }

    // MAX aggregate on FILE column
    test {
        sql "SELECT MAX(`file`) FROM test_fileset_tbl"
        exception "don't support"
    }

    // JOIN on FILE column
    test {
        sql """SELECT * FROM test_fileset_tbl t1
               JOIN test_fileset_tbl t2 ON t1.`file` = t2.`file`"""
        exception "file type could not in join equal conditions"
    }

    // ============ DML Restriction Tests ============
    // FilesetTable is read-only, all write operations should fail.

    // INSERT INTO fileset table
    test {
        sql "INSERT INTO test_fileset_tbl VALUES (NULL)"
        exception "insert into"
    }

    // DELETE from fileset table
    test {
        sql "DELETE FROM test_fileset_tbl WHERE 1=1"
        exception "olap table"
    }

    // UPDATE fileset table
    test {
        sql "UPDATE test_fileset_tbl SET `file` = NULL"
        exception "olapTable"
    }

    // ============ DDL Restriction Tests ============
    // FilesetTable should not support schema change, index, partition, etc.

    // ALTER TABLE ADD COLUMN
    test {
        sql "ALTER TABLE test_fileset_tbl ADD COLUMN c2 INT"
        exception ""
    }

    // ALTER TABLE DROP COLUMN
    test {
        sql "ALTER TABLE test_fileset_tbl DROP COLUMN `file`"
        exception ""
    }

    // ALTER TABLE MODIFY COLUMN
    test {
        sql "ALTER TABLE test_fileset_tbl MODIFY COLUMN `file` VARCHAR(100)"
        exception ""
    }

    // TRUNCATE TABLE
    test {
        sql "TRUNCATE TABLE test_fileset_tbl"
        exception ""
    }

    // CREATE INDEX on fileset table
    test {
        sql "CREATE INDEX idx_file ON test_fileset_tbl(`file`) USING INVERTED"
        exception ""
    }

    // ALTER TABLE ADD PARTITION
    test {
        sql "ALTER TABLE test_fileset_tbl ADD PARTITION p1 VALUES LESS THAN ('100')"
        exception ""
    }

    // ANALYZE TABLE
    test {
        sql "ANALYZE TABLE test_fileset_tbl WITH SYNC"
        exception "fileset table"
    }

    // CREATE MATERIALIZED VIEW (sync MV) on fileset table
    test {
        sql """CREATE MATERIALIZED VIEW test_fileset_mv AS
               SELECT `file` FROM test_fileset_tbl"""
        exception ""
    }

    // EXPORT from fileset table
    test {
        sql """EXPORT TABLE test_fileset_tbl TO "file:///tmp/test_fileset_export/"
               PROPERTIES("format" = "csv")"""
        exception ""
    }

    // Drop tables at end to keep env clean
    sql "DROP TABLE IF EXISTS test_fileset_tbl"
    sql "DROP TABLE IF EXISTS test_fileset_tbl2"
}
