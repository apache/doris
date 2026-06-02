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

// Regression for the V4 (SPIMI) inverted-index write path when the BE-side
// RAM directory is disabled (inverted_index_ram_dir_enable=false).
//
// Bug: with the RAM dir disabled the V4 segment index was written to an
// on-disk temporary DorisFSDirectory, but InvertedIndexColumnWriter::finish()
// (unlike the V1/V2/V3 path that resets _index_writer) left _dir set on
// success, so ~InvertedIndexColumnWriter()->close_on_error()->deleteDirectory()
// wiped that temp dir BEFORE IndexFileWriter packed it into the combined .idx.
// The load still reported success but produced an empty 8-byte .idx, and any
// MATCH query then failed with "IndexInput read past EOF". With the RAM dir
// enabled (default) deleteDirectory() is a no-op so the bug was hidden.
suite("test_inverted_index_v4_ram_dir_off") {

    def tableName = "test_inverted_index_v4_ram_dir_off"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_config = { key, value ->
        for (String backend_id : backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id),
                    backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    // remember the original value so it is restored even on failure
    boolean origRamDirEnable = true
    boolean updated = false
    try {
        String backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id),
                backendId_to_backendHttpPort.get(backend_id))
        assertEquals(code, 0)
        for (Object ele in (List) parseJson(out.trim())) {
            if (((List<String>) ele)[0] == "inverted_index_ram_dir_enable") {
                origRamDirEnable = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }

        // reproduce the bug condition: RAM dir OFF -> on-disk DorisFSDirectory
        set_be_config.call("inverted_index_ram_dir_enable", "false")
        updated = true

        sql "drop table if exists ${tableName}"
        // V4 storage format only supports analyzed fulltext columns.
        sql """
          CREATE TABLE ${tableName} (
            `id` int NULL,
            `content` text NULL,
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true")
          ) ENGINE=OLAP
          DUPLICATE KEY(`id`)
          DISTRIBUTED BY HASH(`id`) BUCKETS 1
          PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2",
            "inverted_index_storage_format" = "V4"
          )
        """

        sql """insert into ${tableName} values
                (1, 'the quick brown fox jumps over the lazy dog'),
                (2, 'apache doris inverted index storage format'),
                (3, 'the spimi writer builds the fulltext index'),
                (4, 'brown fox and lazy dog are friends'),
                (5, 'doris supports fulltext search via inverted index'),
                (6, 'quick search over a large corpus'),
                (7, 'the index must survive when ram dir is off'),
                (8, 'fulltext match query should return correct rows')
        """
        sql """ set enable_common_expr_pushdown = true """

        // Before the fix these MATCH queries errored with "IndexInput read past
        // EOF" (empty .idx); after the fix they return the matching rows.
        qt_match_fox "SELECT id FROM ${tableName} WHERE content MATCH 'fox' ORDER BY id"
        qt_match_the "SELECT id FROM ${tableName} WHERE content MATCH 'the' ORDER BY id"
        qt_match_index "SELECT id FROM ${tableName} WHERE content MATCH 'index' ORDER BY id"
        qt_match_phrase "SELECT id FROM ${tableName} WHERE content MATCH_PHRASE 'lazy dog' ORDER BY id"
        qt_match_all "SELECT id FROM ${tableName} WHERE content MATCH_ALL 'fulltext index' ORDER BY id"
    } finally {
        if (updated) {
            set_be_config.call("inverted_index_ram_dir_enable", origRamDirEnable.toString())
        }
    }
}
