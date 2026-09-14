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

suite("test_index_disk_usage_tvf", "p0,external") {
    String dbName = context.config.getDbNameByFile(context.file)

    def createTable = { String tableName, String format ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                k INT NOT NULL,
                msg VARCHAR(256),
                num INT,
                INDEX idx_msg (msg) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true"),
                INDEX idx_num (num) USING INVERTED
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "${format}"
            )
        """
        // Two loads give two rowsets; 2000 rows keep common SNII terms out of inline postings.
        sql """
            INSERT INTO ${tableName}
            SELECT number, concat('hello world doc', number), number % 97 FROM numbers("number" = "2000")
        """
        sql """
            INSERT INTO ${tableName}
            SELECT number + 2000, concat('quick brown fox doc', number), number % 89 FROM numbers("number" = "2000")
        """
    }

    createTable("index_disk_usage_v2", "V2")
    createTable("index_disk_usage_v3", "V3")
    createTable("index_disk_usage_snii", "SNII")

    order_qt_desc """ DESC FUNCTION index_disk_usage("database" = "${dbName}", "table" = "index_disk_usage_v2") """

    for (String tableName : ["index_disk_usage_v2", "index_disk_usage_v3", "index_disk_usage_snii"]) {
        // Component presence per index and structure; byte values vary across versions.
        order_qt_components """
            SELECT '${tableName}', index_name, column_name, index_type, structure, storage_format, stats_source,
                   SUM(total_bytes) > 0,
                   SUM(dict_bytes) > 0,
                   SUM(posting_bytes) > 0,
                   SUM(position_bytes) IS NULL,
                   SUM(position_bytes) > 0,
                   SUM(stats_bytes) IS NULL,
                   SUM(row_count) = 4000
            FROM index_disk_usage("database" = "${dbName}", "table" = "${tableName}")
            GROUP BY index_name, column_name, index_type, structure, storage_format, stats_source
        """

        // Every rowset's index bytes add up to the index size recorded in its rowset meta; the
        // matched rowset count guards against an empty join.
        order_qt_reconcile """
            SELECT '${tableName}', COUNT(*), SUM(CASE WHEN usage_bytes <> meta_bytes THEN 1 ELSE 0 END)
            FROM (
                SELECT u.rowset_id, SUM(u.total_bytes) AS usage_bytes, MAX(r.INDEX_DISK_SIZE) AS meta_bytes
                FROM index_disk_usage("database" = "${dbName}", "table" = "${tableName}", "level" = "rowset") u
                JOIN information_schema.rowsets r ON u.rowset_id = r.ROWSET_ID
                GROUP BY u.rowset_id
            ) t
        """

        order_qt_levels """
            SELECT '${tableName}',
                (SELECT COUNT(*) FROM index_disk_usage("database" = "${dbName}", "table" = "${tableName}")
                    WHERE rowset_id IS NOT NULL OR segment_id IS NOT NULL),
                (SELECT COUNT(*) FROM index_disk_usage("database" = "${dbName}", "table" = "${tableName}", "level" = "rowset")
                    WHERE rowset_id IS NULL OR segment_id IS NOT NULL),
                (SELECT COUNT(*) FROM index_disk_usage("database" = "${dbName}", "table" = "${tableName}", "level" = "segment")
                    WHERE rowset_id IS NULL OR segment_id IS NULL),
                (SELECT COUNT(DISTINCT rowset_id) FROM index_disk_usage("database" = "${dbName}", "table" = "${tableName}", "level" = "rowset"))
        """
    }

    // position_detail splits SNII positions out of postings without changing the total.
    order_qt_position_detail """
        SELECT d.positions > 0, c.postings = d.postings + d.positions, c.total = d.total
        FROM (
            SELECT SUM(posting_bytes) AS postings, SUM(total_bytes) AS total
            FROM index_disk_usage("database" = "${dbName}", "table" = "index_disk_usage_snii", "indexes" = "idx_msg")
        ) c, (
            SELECT SUM(posting_bytes) AS postings, SUM(position_bytes) AS positions, SUM(total_bytes) AS total
            FROM index_disk_usage("database" = "${dbName}", "table" = "index_disk_usage_snii", "indexes" = "idx_msg",
                                  "position_detail" = "true")
        ) d
    """

    order_qt_index_filter """
        SELECT DISTINCT index_name, structure
        FROM index_disk_usage("database" = "${dbName}", "table" = "index_disk_usage_v3", "indexes" = "idx_num")
    """

    test {
        sql """ SELECT * FROM index_disk_usage("database" = "${dbName}", "table" = "index_disk_usage_v2", "foo" = "bar") """
        exception "'foo' is invalid property"
    }
    test {
        sql """ SELECT * FROM index_disk_usage("database" = "${dbName}", "table" = "index_disk_usage_v2", "level" = "partition") """
        exception "Unsupported level 'partition'"
    }
    test {
        sql """ SELECT * FROM index_disk_usage("database" = "${dbName}", "table" = "index_disk_usage_v2", "partitions" = "p_missing") """
        exception "Unknown partition 'p_missing'"
    }
    test {
        sql """ SELECT * FROM index_disk_usage("database" = "${dbName}", "table" = "index_disk_usage_v2", "indexes" = "idx_missing") """
        exception "Unknown index 'idx_missing'"
    }
    sql "DROP VIEW IF EXISTS index_disk_usage_view"
    sql "CREATE VIEW index_disk_usage_view AS SELECT k FROM index_disk_usage_v2"
    test {
        sql """ SELECT * FROM index_disk_usage("database" = "${dbName}", "table" = "index_disk_usage_view") """
        exception "index_disk_usage only supports OLAP table"
    }
}
