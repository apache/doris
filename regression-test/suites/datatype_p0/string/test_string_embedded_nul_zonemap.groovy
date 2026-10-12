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

// A STRING / VARCHAR value may hold a 0x00 byte. The zone map bound used to be
// parsed back with C string semantics and stopped at that byte, so a comparison pushed into
// the scan read a bound the data never held and dropped pages that hold matching rows.
//
// Each table below holds 32 copies of one value, so the segment min and max are that value.
// The SUM(...) row is the per-row answer, which never goes through the zone map; the three
// COUNT(*) rows run the same predicates through the scan and have to agree with it.
suite("test_string_embedded_nul_zonemap") {
    def load_one_value = { String table, String type, String hex ->
        sql "DROP TABLE IF EXISTS ${table}"
        sql """
            CREATE TABLE ${table} (
                id INT,
                s ${type}
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """
        def rows = (1..32).collect { "(${it}, UNHEX('${hex}'))" }.join(", ")
        sql "INSERT INTO ${table} VALUES ${rows}"
    }

    // 'a' 0x00 'b' in a STRING column -- the 0x00 sits in the middle.
    load_one_value("nul_inside_string", "STRING", "610062")
    qt_inside_string_data "SELECT COUNT(*), MIN(LENGTH(s)), MAX(LENGTH(s)), MIN(HEX(s)), MAX(HEX(s)) FROM nul_inside_string"
    qt_inside_string_oracle "SELECT SUM(CAST(s > 'a' AS INT)), SUM(CAST(s != 'a' AS INT)), SUM(CAST(s <= 'a' AS INT)) FROM nul_inside_string"
    qt_inside_string_gt "SELECT COUNT(*) FROM nul_inside_string WHERE s > 'a'"
    qt_inside_string_ne "SELECT COUNT(*) FROM nul_inside_string WHERE s != 'a'"
    qt_inside_string_le "SELECT COUNT(*) FROM nul_inside_string WHERE s <= 'a'"
    qt_inside_string_minmax "SELECT HEX(MIN(s)), HEX(MAX(s)) FROM nul_inside_string"

    // The same value in a VARCHAR column, which shares the zone map bound path.
    load_one_value("nul_inside_varchar", "VARCHAR(20)", "610062")
    qt_inside_varchar_data "SELECT COUNT(*), MIN(LENGTH(s)), MAX(LENGTH(s)), MIN(HEX(s)), MAX(HEX(s)) FROM nul_inside_varchar"
    qt_inside_varchar_oracle "SELECT SUM(CAST(s > 'a' AS INT)), SUM(CAST(s != 'a' AS INT)), SUM(CAST(s <= 'a' AS INT)) FROM nul_inside_varchar"
    qt_inside_varchar_gt "SELECT COUNT(*) FROM nul_inside_varchar WHERE s > 'a'"
    qt_inside_varchar_ne "SELECT COUNT(*) FROM nul_inside_varchar WHERE s != 'a'"
    qt_inside_varchar_le "SELECT COUNT(*) FROM nul_inside_varchar WHERE s <= 'a'"
    qt_inside_varchar_minmax "SELECT HEX(MIN(s)), HEX(MAX(s)) FROM nul_inside_varchar"

    // 'a' 0x00 -- the 0x00 is the last byte, so the whole bound used to shrink to 'a'.
    load_one_value("nul_trailing_string", "STRING", "6100")
    qt_trailing_string_data "SELECT COUNT(*), MIN(LENGTH(s)), MAX(LENGTH(s)), MIN(HEX(s)), MAX(HEX(s)) FROM nul_trailing_string"
    qt_trailing_string_oracle "SELECT SUM(CAST(s > 'a' AS INT)), SUM(CAST(s != 'a' AS INT)), SUM(CAST(s <= 'a' AS INT)) FROM nul_trailing_string"
    qt_trailing_string_gt "SELECT COUNT(*) FROM nul_trailing_string WHERE s > 'a'"
    qt_trailing_string_ne "SELECT COUNT(*) FROM nul_trailing_string WHERE s != 'a'"
    qt_trailing_string_le "SELECT COUNT(*) FROM nul_trailing_string WHERE s <= 'a'"
    qt_trailing_string_minmax "SELECT HEX(MIN(s)), HEX(MAX(s)) FROM nul_trailing_string"

    // 0x00 'a' -- the 0x00 comes first, so the bound used to shrink to the empty string.
    load_one_value("nul_leading_string", "STRING", "0061")
    qt_leading_string_data "SELECT COUNT(*), MIN(LENGTH(s)), MAX(LENGTH(s)), MIN(HEX(s)), MAX(HEX(s)) FROM nul_leading_string"
    qt_leading_string_oracle "SELECT SUM(CAST(s > 'a' AS INT)), SUM(CAST(s != 'a' AS INT)), SUM(CAST(s <= 'a' AS INT)) FROM nul_leading_string"
    qt_leading_string_gt "SELECT COUNT(*) FROM nul_leading_string WHERE s > 'a'"
    qt_leading_string_ne "SELECT COUNT(*) FROM nul_leading_string WHERE s != 'a'"
    qt_leading_string_le "SELECT COUNT(*) FROM nul_leading_string WHERE s <= 'a'"
    qt_leading_string_minmax "SELECT HEX(MIN(s)), HEX(MAX(s)) FROM nul_leading_string"
}
