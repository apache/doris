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

suite("test_python_udtf_complex") {
    // Test complex UDTF functions using pandas and numpy
    // Dependencies: pandas, numpy

    def pyPath = """${context.file.parent}/py_udf_complex_scripts/py_udf_complex.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())

    try {
        // ========================================
        // Test 1: Expand Date Range
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_expand_date_range(STRING, STRING, STRING); """
        sql """
        CREATE TABLES FUNCTION py_expand_date_range(STRING, STRING, STRING)
        RETURNS ARRAY<STRUCT<date_str:STRING, day_of_week:STRING, is_weekend:BOOLEAN>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_date_range",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS date_range_input; """
        sql """
        CREATE TABLE date_range_input (
            id INT,
            start_date STRING,
            end_date STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO date_range_input VALUES
        (1, '2024-01-01', '2024-01-07'),
        (2, '2024-02-28', '2024-03-02');
        """

        qt_expand_date_range """
            SELECT id, tmp.date_str, tmp.day_of_week, tmp.is_weekend
            FROM date_range_input
            LATERAL VIEW py_expand_date_range(start_date, end_date, 'D') tmp AS date_str, day_of_week, is_weekend
            ORDER BY id, tmp.date_str;
        """

        // ========================================
        // Test 2: Expand Time Slots
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_expand_time_slots(STRING, STRING, INT); """
        sql """
        CREATE TABLES FUNCTION py_expand_time_slots(STRING, STRING, INT)
        RETURNS ARRAY<STRUCT<slot_start:STRING, slot_end:STRING, slot_index:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_time_slots",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_expand_time_slots """
            SELECT tmp.slot_start, tmp.slot_end, tmp.slot_index
            FROM (SELECT 1 AS dummy) t
            LATERAL VIEW py_expand_time_slots('09:00', '12:00', 30) tmp AS slot_start, slot_end, slot_index
            ORDER BY tmp.slot_index;
        """

        // ========================================
        // Test 3: Expand JSON Array
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_expand_json_array(STRING); """
        sql """
        CREATE TABLES FUNCTION py_expand_json_array(STRING)
        RETURNS ARRAY<STRUCT<idx:INT, element:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_json_array",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS json_array_input; """
        sql """
        CREATE TABLE json_array_input (
            id INT,
            json_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO json_array_input VALUES
        (1, '[\"apple\", \"banana\", \"cherry\"]'),
        (2, '[1, 2, 3, 4, 5]'),
        (3, '[{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]');
        """

        qt_expand_json_array """
            SELECT id, tmp.idx, tmp.element
            FROM json_array_input
            LATERAL VIEW py_expand_json_array(json_data) tmp AS idx, element
            ORDER BY id, tmp.idx;
        """

        // ========================================
        // Test 4: Expand JSON Object Keys
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_expand_json_keys(STRING); """
        sql """
        CREATE TABLES FUNCTION py_expand_json_keys(STRING)
        RETURNS ARRAY<STRUCT<json_key:STRING, json_value:STRING, value_type:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_json_object_keys",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS json_object_input; """
        sql """
        CREATE TABLE json_object_input (
            id INT,
            json_obj STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO json_object_input VALUES
        (1, '{\"name\": \"John\", \"age\": 30, \"active\": true}'),
        (2, '{\"city\": \"Beijing\", \"population\": 21540000}');
        """

        qt_expand_json_keys """
            SELECT id, tmp.json_key, tmp.json_value, tmp.value_type
            FROM json_object_input
            LATERAL VIEW py_expand_json_keys(json_obj) tmp AS json_key, json_value, value_type
            ORDER BY id, tmp.json_key;
        """

        // ========================================
        // Test 5: Flatten Nested JSON
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_flatten_json(STRING, INT); """
        sql """
        CREATE TABLES FUNCTION py_flatten_json(STRING, INT)
        RETURNS ARRAY<STRUCT<flat_key:STRING, flat_value:STRING, depth:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.flatten_nested_json",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS nested_json_input; """
        sql """
        CREATE TABLE nested_json_input (
            id INT,
            nested_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO nested_json_input VALUES
        (1, '{\"user\": {\"name\": \"Alice\", \"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}}, \"active\": true}');
        """

        qt_flatten_json """
            SELECT id, tmp.flat_key, tmp.flat_value, tmp.depth
            FROM nested_json_input
            LATERAL VIEW py_flatten_json(nested_data, 10) tmp AS flat_key, flat_value, depth
            ORDER BY tmp.flat_key;
        """

        // ========================================
        // Test 6: Expand Histogram Bins
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_histogram_bins(STRING, INT); """
        sql """
        CREATE TABLES FUNCTION py_histogram_bins(STRING, INT)
        RETURNS ARRAY<STRUCT<bin_index:INT, bin_start:DOUBLE, bin_end:DOUBLE, bin_count:INT, percentage:DOUBLE>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_histogram_bins",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_histogram_bins """
            SELECT tmp.bin_index, tmp.bin_start, tmp.bin_end, tmp.bin_count, tmp.percentage
            FROM (SELECT '[10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]' AS val_list) t
            LATERAL VIEW py_histogram_bins(val_list, 5) tmp AS bin_index, bin_start, bin_end, bin_count, percentage
            ORDER BY tmp.bin_index;
        """

        // ========================================
        // Test 7: Expand Quartile Distribution
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_quartile_distribution(STRING); """
        sql """
        CREATE TABLES FUNCTION py_quartile_distribution(STRING)
        RETURNS ARRAY<STRUCT<quartile_name:STRING, min_value:DOUBLE, max_value:DOUBLE, item_count:INT, mean_value:DOUBLE>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_quartile_distribution",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_quartile_distribution """
            SELECT tmp.quartile_name, tmp.min_value, tmp.max_value, tmp.item_count, tmp.mean_value
            FROM (SELECT '[1, 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100]' AS val_list) t
            LATERAL VIEW py_quartile_distribution(val_list) tmp AS quartile_name, min_value, max_value, item_count, mean_value
            ORDER BY tmp.min_value;
        """

        // ========================================
        // Test 8: Expand N-Grams
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_expand_ngrams(STRING, INT); """
        sql """
        CREATE TABLES FUNCTION py_expand_ngrams(STRING, INT)
        RETURNS ARRAY<STRUCT<ngram_index:INT, ngram_text:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_ngrams",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS text_ngram_input; """
        sql """
        CREATE TABLE text_ngram_input (
            id INT,
            sentence STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO text_ngram_input VALUES
        (1, 'the quick brown fox jumps over the lazy dog');
        """

        qt_expand_ngrams """
            SELECT id, tmp.ngram_index, tmp.ngram_text
            FROM text_ngram_input
            LATERAL VIEW py_expand_ngrams(sentence, 3) tmp AS ngram_index, ngram_text
            ORDER BY tmp.ngram_index;
        """

        // ========================================
        // Test 9: Expand Sentences
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_expand_sentences(STRING); """
        sql """
        CREATE TABLES FUNCTION py_expand_sentences(STRING)
        RETURNS ARRAY<STRUCT<sentence_index:INT, sentence:STRING, word_count:INT, char_count:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_sentences",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_expand_sentences """
            SELECT tmp.sentence_index, tmp.sentence, tmp.word_count, tmp.char_count
            FROM (SELECT 'Hello world. This is a test! How are you?' AS text) t
            LATERAL VIEW py_expand_sentences(text) tmp AS sentence_index, sentence, word_count, char_count
            ORDER BY tmp.sentence_index;
        """

        // ========================================
        // Test 10: Expand Order Items
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_expand_order_items(STRING); """
        sql """
        CREATE TABLES FUNCTION py_expand_order_items(STRING)
        RETURNS ARRAY<STRUCT<order_id:STRING, item_index:INT, sku:STRING, quantity:INT, unit_price:DOUBLE, line_total:DOUBLE>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_order_items",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS orders_input; """
        sql """
        CREATE TABLE orders_input (
            id INT,
            order_json STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO orders_input VALUES
        (1, '{\"order_id\": \"ORD001\", \"items\": [{\"sku\": \"PROD-A\", \"qty\": 2, \"price\": 10.0}, {\"sku\": \"PROD-B\", \"qty\": 1, \"price\": 25.0}]}'),
        (2, '{\"order_id\": \"ORD002\", \"items\": [{\"sku\": \"PROD-C\", \"qty\": 3, \"price\": 15.0}]}');
        """

        qt_expand_order_items """
            SELECT id, tmp.order_id, tmp.item_index, tmp.sku, tmp.quantity, tmp.unit_price, tmp.line_total
            FROM orders_input
            LATERAL VIEW py_expand_order_items(order_json) tmp AS order_id, item_index, sku, quantity, unit_price, line_total
            ORDER BY id, tmp.item_index;
        """

        // ========================================
        // Test 11: Expand User Events
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_expand_user_events(STRING); """
        sql """
        CREATE TABLES FUNCTION py_expand_user_events(STRING)
        RETURNS ARRAY<STRUCT<event_index:INT, event_type:STRING, event_ts:STRING, event_data:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_user_events",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS events_input; """
        sql """
        CREATE TABLE events_input (
            user_id INT,
            events_json STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO events_input VALUES
        (1, '[{\"event\": \"click\", \"timestamp\": \"2024-01-01 10:00:00\", \"data\": {\"page\": \"home\"}}, {\"event\": \"purchase\", \"timestamp\": \"2024-01-01 10:05:00\", \"data\": {\"amount\": 100}}]');
        """

        qt_expand_user_events """
            SELECT user_id, tmp.event_index, tmp.event_type, tmp.event_ts, tmp.event_data
            FROM events_input
            LATERAL VIEW py_expand_user_events(events_json) tmp AS event_index, event_type, event_ts, event_data
            ORDER BY tmp.event_index;
        """

        // ========================================
        // Test 12: Expand BBox to Tiles
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_bbox_to_tiles(DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE); """
        sql """
        CREATE TABLES FUNCTION py_bbox_to_tiles(DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE)
        RETURNS ARRAY<STRUCT<tile_row:INT, tile_col:INT, min_lat:DOUBLE, min_lon:DOUBLE, max_lat:DOUBLE, max_lon:DOUBLE>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udtf.expand_bbox_to_tiles",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_bbox_to_tiles """
            SELECT tmp.tile_row, tmp.tile_col, tmp.min_lat, tmp.min_lon, tmp.max_lat, tmp.max_lon
            FROM (SELECT 39.9 AS min_lat, 116.3 AS min_lon, 40.1 AS max_lat, 116.5 AS max_lon) t
            LATERAL VIEW py_bbox_to_tiles(min_lat, min_lon, max_lat, max_lon, 0.1) tmp AS tile_row, tile_col, min_lat, min_lon, max_lat, max_lon
            ORDER BY tmp.tile_row, tmp.tile_col;
        """

        // ========================================
        // Test 13: NULL Input Handling
        // ========================================
        qt_null_json_array """
            SELECT tmp.idx, tmp.element
            FROM (SELECT NULL AS json_data) t
            LATERAL VIEW py_expand_json_array(json_data) tmp AS idx, element;
        """

        qt_null_date_range """
            SELECT tmp.date_str
            FROM (SELECT NULL AS start_date, '2024-01-07' AS end_date) t
            LATERAL VIEW py_expand_date_range(start_date, end_date, 'D') tmp AS date_str, day_of_week, is_weekend;
        """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_expand_date_range(STRING, STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_expand_time_slots(STRING, STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_expand_json_array(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_expand_json_keys(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_flatten_json(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_histogram_bins(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_quartile_distribution(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_expand_ngrams(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_expand_sentences(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_expand_order_items(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_expand_user_events(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_bbox_to_tiles(DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE);")
        try_sql("DROP TABLE IF EXISTS date_range_input;")
        try_sql("DROP TABLE IF EXISTS json_array_input;")
        try_sql("DROP TABLE IF EXISTS json_object_input;")
        try_sql("DROP TABLE IF EXISTS nested_json_input;")
        try_sql("DROP TABLE IF EXISTS text_ngram_input;")
        try_sql("DROP TABLE IF EXISTS orders_input;")
        try_sql("DROP TABLE IF EXISTS events_input;")
    }
}
