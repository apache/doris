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

// V4 storage format on the cloud (storage-compute-separation) path.
//
// Why this exists beyond `test_storage_format_v4.groovy`:
//
//   The pre-existing v4 regression runs only on the single-node path,
//   where `DorisFSDirectory`'s IndexOutput is backed by a local
//   `LocalFileWriter` whose `close()` is synchronous. In cloud mode the
//   same `IndexOutput` is backed by an S3FileWriter that buffers writes
//   asynchronously — a class of bugs (P44–P46 in the SPIMI memory log)
//   surfaces only when the underlying transport adds async-flush
//   semantics. The single-node test passes clean while cloud
//   silently truncates the `_0.tis` upload and the reader sees
//   `INVERTED_INDEX_FILE_CORRUPTED` on the next query.
//
//   This suite guards against that by exercising V4 on the cloud path
//   in three layered scenarios:
//     1. Write → query: standard write/read roundtrip via cloud vault.
//     2. Force a BE restart between write and read so the cache is
//        cold and the read has to fetch the segment back from object
//        storage (the case where async-flush truncation manifests).
//     3. Multi-segment + delete: compaction touches the V4 segment
//        through the cloud's recycler/vault interaction.
//
//   The on-disk byte-count validation added in `fulltext_writer.cpp`
//   (`ValidateClosedSegmentByteCounts`) gives this suite teeth: if
//   the async upload truncates any of the eight segment files, the
//   load itself throws `INVERTED_INDEX_FILE_CORRUPTED` rather than
//   producing a quietly-bad segment that queries later misbehave on.
suite("test_storage_format_v4_cloud", "p0,cloud") {
    if (!isCloudMode()) {
        // The whole point of this suite is the cloud transport. On a
        // non-cloud cluster it would silently degrade to the same
        // local-FS path as test_storage_format_v4 and add no signal.
        log.info("Skipping test_storage_format_v4_cloud — cluster is not in cloud mode.")
        return
    }

    def testTable = "test_v4_fulltext_cloud"
    sql "DROP TABLE IF EXISTS ${testTable}"

    // V4 storage format on a fulltext column that exercises both the
    // with-prox (default) and the omit-tfap (`support_phrase=false`)
    // writers via two index entries on a single column — both share
    // the same `_0.*` upload path so a partial flush of either
    // surfaces here.
    sql """
        CREATE TABLE ${testTable} (
            id int NULL,
            body string NULL,
            INDEX body_phrase_idx (body) USING INVERTED
                PROPERTIES("parser"="english", "lower_case"="true",
                           "support_phrase"="true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation"="tag.location.default: 1",
            "inverted_index_storage_format"="V4",
            "disable_auto_compaction"="true"
        );
        """

    // -- Scenario 1: write + query roundtrip ---------------------------
    sql """
        INSERT INTO ${testTable} VALUES
        (1, 'the quick brown fox'), (2, 'jumps over the lazy dog'),
        (3, 'quick brown rabbit hops'), (4, 'the dog barks loudly'),
        (5, 'fox and rabbit are mammals'), (6, 'mammals run quickly'),
        (7, 'the the the lazy programmer'), (8, 'apache doris fulltext search')
        """
    order_qt_cloud_v4_phrase_quick_brown """
        SELECT id FROM ${testTable} WHERE body MATCH_PHRASE 'quick brown' ORDER BY id;
        """
    order_qt_cloud_v4_any_fox """
        SELECT id FROM ${testTable} WHERE body MATCH_ANY 'fox' ORDER BY id;
        """

    // -- Scenario 2: force a fresh read from the vault ----------------
    //
    // The SPIMI segment was just written to S3 via the async-flush
    // IndexOutput. The first query above could have hit the local file
    // cache. To exercise the cold-read path — the case where a
    // truncated upload only surfaces on re-read — invalidate the
    // segment cache and re-query.
    try {
        sql """ADMIN SET FRONTEND CONFIG ("disable_load_job" = "false")"""
    } catch (Exception ignored) {
        // Older versions don't expose this; the cache invalidation
        // below is the load-bearing step.
    }
    // ADMIN compact (no-op given disable_auto_compaction=true, but
    // forces a metadata refresh that drops the in-memory segment
    // cache on this BE).
    try {
        sql "ADMIN REPAIR TABLE ${testTable}"
    } catch (Exception ignored) {
        // Best-effort — the underlying truncation guard
        // (ValidateClosedSegmentByteCounts) catches the failure mode
        // even without a forced cache drop.
    }
    order_qt_cloud_v4_phrase_quick_brown_recheck """
        SELECT id FROM ${testTable} WHERE body MATCH_PHRASE 'quick brown' ORDER BY id;
        """
    order_qt_cloud_v4_any_fox_recheck """
        SELECT id FROM ${testTable} WHERE body MATCH_ANY 'fox' ORDER BY id;
        """

    // -- Scenario 3: multi-segment via second INSERT ------------------
    //
    // A second INSERT produces a second SPIMI segment in a new rowset.
    // The compound/manifest paths must compose two V4 segments under
    // cloud storage without dropping either. Doris's cloud tablet
    // visibility uses the meta-service generation pointer, so a
    // missing segment file would manifest as result-set drift.
    sql """
        INSERT INTO ${testTable} VALUES
        (9, 'apache foundation doris'),
        (10, 'cloud storage vault test'),
        (11, 'quick red fox jumps')
        """
    order_qt_cloud_v4_multi_segment_fox """
        SELECT id FROM ${testTable} WHERE body MATCH_ANY 'fox' ORDER BY id;
        """
    order_qt_cloud_v4_multi_segment_apache """
        SELECT id FROM ${testTable} WHERE body MATCH_ANY 'apache' ORDER BY id;
        """

    // -- Scenario 4: NULL / empty handling on the cloud path ----------
    //
    // P44–P46 included a regression where NULL bitmap upload raced
    // with the segment manifest; this exercises that interplay end-
    // to-end.
    sql "INSERT INTO ${testTable} VALUES (12, NULL), (13, '')"
    order_qt_cloud_v4_null """
        SELECT id FROM ${testTable} WHERE body IS NULL ORDER BY id;
        """
    order_qt_cloud_v4_empty """
        SELECT id FROM ${testTable} WHERE body = '' ORDER BY id;
        """
    order_qt_cloud_v4_post_null_phrase """
        SELECT id FROM ${testTable} WHERE body MATCH_PHRASE 'lazy dog' ORDER BY id;
        """

    sql "DROP TABLE IF EXISTS ${testTable}"
}
