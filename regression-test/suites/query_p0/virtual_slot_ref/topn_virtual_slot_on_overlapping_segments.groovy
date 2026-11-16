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

// Storage Top-N (VCollectIterator::_topn_next) over a rowset whose segments overlap
// reads through the ordered merge iterator. When the scan carries virtual columns
// (CSE virtual slots or ANN distance), the merge iterator used to copy rows into the
// caller's block whose virtual slots were still ColumnNothing placeholders, dropping
// the values, and the following MutableBlock::add_rows failed with E-7412.
// https://github.com/apache/doris/issues/70489
suite("topn_virtual_slot_on_overlapping_segments", "nonConcurrent") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    // The data rowset must contain several segments whose key ranges overlap,
    // otherwise the rowset reader uses the union iterator and the bug path is not reached.
    def assertOverlappingRowset = { String tableName ->
        def tablets = sql_return_maparray "show tablets from ${tableName}"
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        def (code, out, err) = be_show_tablet_status(
                backendId_to_backendIP.get(tablet.BackendId),
                backendId_to_backendHttpPort.get(tablet.BackendId), tablet.TabletId)
        logger.info("tablet ${tablet.TabletId} status: code=${code}, out=${out}, err=${err}")
        assertEquals(0, code)
        // rowset format: "[2-2] 7 DATA OVERLAPPING <rowset_id> <size> level=0"
        def dataRowsets = parseJson(out.trim()).rowsets.findAll { !it.startsWith("[0-1]") }
        assertEquals(1, dataRowsets.size())
        def fields = dataRowsets[0].split(" ")
        assertTrue(fields[1].toInteger() > 1, "expect multiple segments: ${dataRowsets[0]}")
        assertEquals("OVERLAPPING", fields[3])
    }

    sql "set experimental_enable_virtual_slot_for_cse = true"
    sql "set enable_segment_limit_pushdown = true"

    def cseQuery = { String table, String order -> """
            select id, abs(v - 500) * 3 + 1 as x from ${table}
            where abs(v - 500) * 3 + 1 > 10 and abs(v - 500) * 3 + 1 < 1000
            order by id ${order} limit 20
        """ }
    def annQuery = { String order -> """
            select id, l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) as d
            from topn_vslot_overlap_ann
            where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < 100
            order by id ${order} limit 20
        """ }

    // A small memtable makes one load flush several segments, and the shuffled keys make
    // their key ranges overlap. Keep segcompaction from merging them during the load.
    setBeConfigTemporary([
            write_buffer_size                : 262144,
            enable_adaptive_write_buffer_size: false,
            segcompaction_batch_size         : 1000,
    ]) {
        // 1. CSE virtual slot on a duplicate-key table.
        sql "drop table if exists topn_vslot_overlap_dup"
        sql """
            create table topn_vslot_overlap_dup (
                id int not null,
                v int not null
            ) duplicate key(id)
            distributed by hash(id) buckets 1
            properties ("replication_num" = "1", "disable_auto_compaction" = "true")
        """
        sql """
            insert into topn_vslot_overlap_dup
            select cast((number * 7919) % 300000 as int), cast(number % 1000 as int)
            from numbers("number" = "300000")
        """

        // 2. CSE virtual slot on a merge-on-write unique-key table. Its plain scan does not
        //    need ordered rowset reads, but storage Top-N does, so it also reaches the merge
        //    iterator.
        sql "drop table if exists topn_vslot_overlap_mow"
        sql """
            create table topn_vslot_overlap_mow (
                id int not null,
                v int not null
            ) unique key(id)
            distributed by hash(id) buckets 1
            properties ("replication_num" = "1", "disable_auto_compaction" = "true",
                        "enable_unique_key_merge_on_write" = "true")
        """
        sql """
            insert into topn_vslot_overlap_mow
            select cast((number * 7919) % 300000 as int), cast(number % 1000 as int)
            from numbers("number" = "300000")
        """

        // 3. ANN distance as virtual slot.
        sql "drop table if exists topn_vslot_overlap_ann"
        sql """
            create table topn_vslot_overlap_ann (
                id int not null,
                embedding array<float> not null,
                index idx_emb (embedding) using ann properties(
                    "index_type" = "hnsw", "metric_type" = "l2_distance", "dim" = "4")
            ) duplicate key(id)
            distributed by hash(id) buckets 1
            properties ("replication_num" = "1", "disable_auto_compaction" = "true")
        """
        sql """
            insert into topn_vslot_overlap_ann
            select cast((number * 7919) % 50000 as int),
                   array(cast(number % 97 as float), cast(number % 89 as float),
                         cast(number % 83 as float), cast(number % 79 as float))
            from numbers("number" = "50000")
        """
    }

    assertOverlappingRowset("topn_vslot_overlap_dup")
    assertOverlappingRowset("topn_vslot_overlap_mow")
    assertOverlappingRowset("topn_vslot_overlap_ann")

    // The plans must push a virtual column and a key Top-N into the OLAP scan.
    for (def query : [cseQuery("topn_vslot_overlap_dup", "asc"),
                      cseQuery("topn_vslot_overlap_mow", "asc"),
                      annQuery("asc")]) {
        explain {
            sql query
            verbose true
            contains "SORT LIMIT: 20"
            contains "__DORIS_VIRTUAL_COL__"
        }
    }

    qt_dup_asc cseQuery("topn_vslot_overlap_dup", "asc")
    qt_dup_desc cseQuery("topn_vslot_overlap_dup", "desc")
    qt_mow_asc cseQuery("topn_vslot_overlap_mow", "asc")
    qt_mow_desc cseQuery("topn_vslot_overlap_mow", "desc")
    qt_ann_asc annQuery("asc")
    qt_ann_desc annQuery("desc")
}
