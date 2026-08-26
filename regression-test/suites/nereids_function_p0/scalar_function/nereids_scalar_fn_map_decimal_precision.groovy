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

// The key and the value of a MAP are independent decimal slots. The default
// decimal v3 precision promotion must keep their precision/scale independent
// instead of merging them into one type, otherwise widening the scale of a big
// integral key silently converts it to NULL (e.g. in map_keys) and UNNEST(MAP)/
// explode_map may fail the type check.
suite("nereids_scalar_fn_map_decimal_precision") {
    sql "set enable_decimal256 = true;"

    // 1. map_keys must keep the big integral key instead of turning it into NULL
    order_qt_map_keys_decimal256 """
        SELECT
            CAST(MAP(
                CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
                CAST('0.125000000000000000' AS DECIMAL(76,18))
            ) AS STRING) AS source_map,
            CAST(MAP_KEYS(MAP(
                CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
                CAST('0.125000000000000000' AS DECIMAL(76,18))
            )) AS STRING) AS actual_keys,
            CAST(ARRAY(CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0))) AS STRING) AS expected_keys;
    """

    // 2. UNNEST(MAP) must pass the type check and return independent key/value
    order_qt_unnest_map_decimal256 """
        SELECT key_out, value_out
        FROM UNNEST(MAP(
            CAST('1.2500' AS DECIMAL(16,4)),
            CAST('2.125000000000000000' AS DECIMAL(76,18))
        )) AS expanded(key_out, value_out);
    """

    // 3. explode_map keeps independent key/value decimals
    order_qt_explode_map_decimal256 """
        SELECT k, v
        FROM (SELECT 1) x
        LATERAL VIEW EXPLODE_MAP(MAP(
            CAST('1.2500' AS DECIMAL(16,4)),
            CAST('2.125000000000000000' AS DECIMAL(76,18))
        )) t AS k, v;
    """

    // 4. map_contains_key can find the big integral key
    order_qt_map_contains_key_decimal256 """
        SELECT MAP_CONTAINS_KEY(MAP(
            CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
            CAST('0.125000000000000000' AS DECIMAL(76,18))
        ), CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0))) AS ck;
    """

    // 5. map_values keeps the value
    order_qt_map_values_decimal256 """
        SELECT CAST(MAP_VALUES(MAP(
            CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
            CAST('0.125000000000000000' AS DECIMAL(76,18))
        )) AS STRING) AS actual_values;
    """

    // 6. map_entries keeps independent key/value decimals
    order_qt_map_entries_decimal256 """
        SELECT CAST(MAP_ENTRIES(MAP(
            CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
            CAST('0.125000000000000000' AS DECIMAL(76,18))
        )) AS STRING) AS entries;
    """

    // 7. element_at over a map with independent key/value decimals
    order_qt_element_at_decimal256 """
        SELECT ELEMENT_AT(MAP(
            CAST('1.2500' AS DECIMAL(16,4)),
            CAST('2.125000000000000000' AS DECIMAL(76,18))
        ), CAST('1.2500' AS DECIMAL(16,4))) AS value;
    """

    // 8. basic decimal v3 precision promotion (single slot) is not affected
    order_qt_basic_decimal256 """
        SELECT ABS(CAST('123.456' AS DECIMAL(10,3))) AS abs_v,
               ROUND(CAST('123.456' AS DECIMAL(10,3)), 2) AS round_v;
    """
}
