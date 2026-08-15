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

import org.apache.doris.regression.action.ProfileAction

suite("variant_shredded_state_v2", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        assertTrue(getFeConfig("enable_variant_v2").toBoolean())

        sql "SET default_variant_enable_doc_mode = false"
        sql "SET default_variant_enable_typed_paths_to_sparse = false"
        sql "SET default_variant_max_sparse_column_statistics_size = 10000"
        sql "SET default_variant_sparse_hash_shard_count = 1"
        sql "DROP TABLE IF EXISTS variant_shredded_state_v2"
        sql """
            CREATE TABLE variant_shredded_state_v2 (
                id INT,
                v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "16")> NULL
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        // Keep all conflict forms in one writer batch so the reader receives one fixed S layout.
        sql """
            INSERT INTO variant_shredded_state_v2 VALUES
                (1, parse_to_variant('{"same":1,"type_conflict":10,"exact_shape":7,"ancestor":9,"deep":{"mid":{"leaf":101}},"triad":null}')),
                (2, parse_to_variant('{"same":2,"type_conflict":"ten","exact_shape":{"leaf":20},"ancestor":{"child":30},"deep":{"mid":202}}')),
                (3, parse_to_variant('{"same":3,"type_conflict":20,"exact_shape":8,"ancestor":{"child":40},"deep":303,"triad":42}')),
                (4, parse_to_variant('{"same":4,"type_conflict":"twenty","exact_shape":{"leaf":50},"ancestor":11,"deep":{"mid":{"leaf":"four"}}}')),
                (5, parse_to_variant('{"same":5}')),
                (6, NULL),
                (7, parse_to_variant('null'))
        """

        order_qt_shredded_root """
            SELECT id, CAST(v AS STRING), v IS NULL
            FROM variant_shredded_state_v2
            ORDER BY id
        """

        order_qt_shredded_conflicts """
            SELECT id,
                   CAST(v['same'] AS BIGINT),
                   CAST(v['type_conflict'] AS STRING),
                   CAST(v['exact_shape'] AS STRING),
                   CAST(v['exact_shape']['leaf'] AS BIGINT),
                   CAST(v['ancestor'] AS STRING),
                   CAST(v['ancestor']['child'] AS BIGINT)
            FROM variant_shredded_state_v2
            ORDER BY id
        """

        order_qt_shredded_null_states """
            SELECT id,
                   v IS NULL,
                   v['triad'] IS NULL,
                   CAST(v['triad'] AS STRING),
                   variant_type(v['triad'])
            FROM variant_shredded_state_v2
            ORDER BY id
        """

        order_qt_shredded_three_segment_path """
            SELECT id,
                   CAST(v['deep'] AS STRING),
                   CAST(v['deep']['mid'] AS STRING),
                   CAST(v['deep']['mid']['leaf'] AS STRING),
                   variant_type(v['deep']['mid']['leaf'])
            FROM variant_shredded_state_v2
            ORDER BY id
        """

        // Exercise whole-value filtering and permutation before the scalar fallback at the result.
        order_qt_shredded_filter_permute """
            SELECT id, CAST(v AS STRING)
            FROM variant_shredded_state_v2
            WHERE id IN (1, 3, 5, 7)
            ORDER BY id DESC
        """

        sql "SET enable_profile = true"
        sql "SET profile_level = 2"
        def profileToken = "variant_v2_shredded_" + UUID.randomUUID().toString()
        sql """
            SELECT "${profileToken}", id, CAST(v AS STRING)
            FROM variant_shredded_state_v2
            ORDER BY id
        """

        def profileAction = new ProfileAction(context)
        String profile = profileAction.getProfileBySql(
                profileToken, ["VariantV2ShreddedOutputRows"])
        def shreddedRowsMatcher =
                (profile =~ /VariantV2ShreddedOutputRows:\s*([0-9,]+)/)
        long shreddedRows = 0
        while (shreddedRowsMatcher.find()) {
            shreddedRows += shreddedRowsMatcher.group(1).replace(",", "") as long
        }
        assertTrue(shreddedRows > 0,
                "expected a real shredded Variant V2 reader output, profile:\n${profile}")

        sql "SET default_variant_enable_doc_mode = true"
        sql "DROP TABLE IF EXISTS variant_shredded_doc_state_v2"
        sql """
            CREATE TABLE variant_shredded_doc_state_v2 (
                id INT,
                v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "1")> NULL
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        sql """
            INSERT INTO variant_shredded_doc_state_v2 VALUES
                (1, parse_to_variant('{"hot":1,"deep":{"leaf":10},"cold":{"x":1}}')),
                (2, parse_to_variant('{"hot":2,"deep":{"leaf":"ten"},"cold":{"y":2}}')),
                (3, parse_to_variant('{"hot":3,"deep":{"leaf":{"z":3}},"cold":{"z":3}}')),
                (4, NULL)
        """

        order_qt_shredded_doc_mode """
            SELECT id,
                   CAST(v AS STRING),
                   CAST(v['hot'] AS BIGINT),
                   CAST(v['deep']['leaf'] AS STRING),
                   v IS NULL
            FROM variant_shredded_doc_state_v2
            ORDER BY id
        """

    }
}
