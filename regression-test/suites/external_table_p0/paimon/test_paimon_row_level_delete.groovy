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

// Row-level DELETE / UPDATE / MERGE on Paimon 2.0 external tables:
//   - primary-key table: DELETE, UPDATE and MERGE supported. A delete is a keyed RowKind.DELETE record
//     the merge engine cancels against the key; UPDATE/MERGE arrive as an operation-tagged stream whose
//     tags the writer maps to keyed upserts (1/3/4) and deletes (2/5).
//   - unaware-bucket append-only table WITH deletion vectors: DELETE only. The scan projects the
//     synthetic row locator (file_path, row_position) and the writer records the removed positions in a
//     deletion-vector index, merged with existing vectors. UPDATE/MERGE on any append-only shape stay
//     rejected: they need a combined vector-plus-append write the writer does not implement.
//   - append-only table WITHOUT deletion vectors: rejected (a Paimon requirement — nowhere to record the
//     removal). Bucketed append (a pinned bucket count) is rejected too: the vector must be filed under
//     the file's REAL bucket, which the locator does not carry yet.
//
// Every supported case asserts on the surviving ROWS, not just on the statement succeeding: a delete that
// silently dropped too much (or nothing) would otherwise pass.
suite("test_paimon_row_level_delete",
        "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_row_level_delete"
    String dbName = "paimon_row_level_delete_db"
    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'meta.cache.paimon.table.ttl-second'='0'
        )
    """

    try {
        sql """switch ${catalogName}"""
        sql """create database if not exists ${dbName}"""
        sql """use ${dbName}"""

        // ==================== primary-key table ====================

        sql """drop table if exists pk_delete"""
        sql """
            create table pk_delete (
                id int not null,
                grp int,
                note string
            ) engine=paimon properties (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet'
            )
        """
        sql """
            insert into pk_delete values
                (1, 10, 'one'), (2, 10, 'two'), (3, 20, 'three'), (4, 20, 'four'), (5, 30, 'five')
        """
        order_qt_pk_before """select id, grp, note from pk_delete"""

        // Single-row delete by key.
        sql """delete from pk_delete where id = 1"""
        order_qt_pk_after_single """select id, grp, note from pk_delete"""

        // Multi-row delete by a non-key predicate: the plan must find the keys itself.
        sql """delete from pk_delete where grp = 20"""
        order_qt_pk_after_multi """select id, grp, note from pk_delete"""

        // A predicate matching nothing must be a no-op, not a full-table wipe.
        sql """delete from pk_delete where id = 999"""
        order_qt_pk_after_nomatch """select id, grp, note from pk_delete"""

        // Deleting an already-deleted key must stay a no-op (the DELETE record is idempotent).
        sql """delete from pk_delete where id = 1"""
        order_qt_pk_after_redelete """select id, grp, note from pk_delete"""

        // A delete commits a snapshot even though it writes no user-visible data file.
        qt_pk_snapshot_kinds """
            select count(*) from pk_delete\$snapshots
        """

        // Re-inserting a deleted key must resurrect it (the tombstone must not shadow the new row).
        sql """insert into pk_delete values (1, 10, 'one-again')"""
        order_qt_pk_after_reinsert """select id, grp, note from pk_delete"""

        // ==================== append-only table WITH deletion vectors ====================

        sql """drop table if exists dv_delete"""
        // Unaware-bucket (no pinned bucket count) is the shape the deletion-vector writer supports: the
        // removed row's position is recorded in a DV index keyed by (data file, ordinal).
        sql """
            create table dv_delete (
                id int not null,
                grp int,
                note string
            ) engine=paimon properties (
                'file.format'='parquet',
                'deletion-vectors.enabled'='true'
            )
        """
        sql """
            insert into dv_delete values
                (1, 10, 'one'), (2, 10, 'two'), (3, 20, 'three'), (4, 20, 'four')
        """
        order_qt_dv_before """select id, grp, note from dv_delete"""

        // With no key to cancel against, the deleted rows are marked by position in a deletion vector.
        sql """delete from dv_delete where grp = 10"""
        order_qt_dv_after_delete """select id, grp, note from dv_delete"""

        // Rows appended AFTER a delete must be unaffected by the existing deletion vector.
        sql """insert into dv_delete values (5, 30, 'five')"""
        order_qt_dv_after_append """select id, grp, note from dv_delete"""

        // A second delete must MERGE into the existing vector, not overwrite it — id=1/2 must stay gone.
        sql """delete from dv_delete where id = 3"""
        order_qt_dv_after_second_delete """select id, grp, note from dv_delete"""


        // ---- bucketed append (pinned bucket count) stays rejected even with vectors ----
        sql """drop table if exists dv_bucketed"""
        sql """
            create table dv_bucketed (
                id int not null,
                note string
            ) engine=paimon properties (
                'bucket'='2',
                'bucket-key'='id',
                'file.format'='parquet',
                'deletion-vectors.enabled'='true'
            )
        """
        sql """insert into dv_bucketed values (1, 'one')"""
        test {
            sql """delete from dv_bucketed where id = 1"""
            exception "unaware-bucket"
        }

        // ==================== append-only table WITHOUT deletion vectors ====================

        sql """drop table if exists plain_append"""
        sql """
            create table plain_append (
                id int not null,
                note string
            ) engine=paimon properties (
                'bucket'='1',
                'bucket-key'='id',
                'file.format'='parquet'
            )
        """
        sql """insert into plain_append values (1, 'one'), (2, 'two')"""

        // Same rejection as the deletion-vector table above: without a key there is no way to express the
        // removal that this connector can currently write.
        test {
            sql """delete from plain_append where id = 1"""
            exception "append-only"
        }
        // The rejected statement must not have partially applied.
        order_qt_plain_append_untouched """select id, note from plain_append"""

        // ==================== UPDATE on a primary-key table ====================
        // The statement arrives at the writer as an operation-tagged merge stream; on a primary-key
        // table every non-delete tag is a keyed upsert, which IS the update.

        sql """drop table if exists pk_update"""
        sql """
            create table pk_update (
                id int not null,
                grp int,
                note string
            ) engine=paimon properties (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet'
            )
        """
        sql """insert into pk_update values (1, 10, 'one'), (2, 10, 'two'), (3, 20, 'three')"""
        order_qt_update_before """select id, grp, note from pk_update"""

        sql """update pk_update set note = 'one-updated' where id = 1"""
        order_qt_update_after_single """select id, grp, note from pk_update"""

        // Multi-row update by a non-key predicate: every matched row must change, and only those.
        sql """update pk_update set note = 'grp10' where grp = 10"""
        order_qt_update_after_multi """select id, grp, note from pk_update"""

        // Updating a non-key column must not change the row count — a stream that appended instead of
        // upserting would silently double the rows here.
        qt_update_row_count """select count(*) from pk_update"""

        // A predicate matching nothing must leave the table untouched.
        sql """update pk_update set note = 'never' where id = 999"""
        order_qt_update_after_nomatch """select id, grp, note from pk_update"""

        // ==================== MERGE INTO a primary-key table ====================

        sql """drop table if exists merge_target"""
        sql """
            create table merge_target (
                id int not null,
                note string
            ) engine=paimon properties (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet'
            )
        """
        sql """insert into merge_target values (1, 'target-one'), (2, 'target-two')"""

        sql """drop table if exists merge_source"""
        sql """
            create table merge_source (
                id int not null,
                note string
            ) engine=paimon properties (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet'
            )
        """
        // id=2 matches (update), id=3 does not (insert).
        sql """insert into merge_source values (2, 'source-two'), (3, 'source-three')"""
        order_qt_merge_before """select id, note from merge_target"""

        sql """
            merge into merge_target t
            using merge_source s
            on t.id = s.id
            when matched then update set t.note = s.note
            when not matched then insert (id, note) values (s.id, s.note)
        """
        // Expect exactly: 1 untouched, 2 updated from source, 3 inserted.
        order_qt_merge_after """select id, note from merge_target"""

        // A matched-DELETE clause maps to RowKind.DELETE rows: id=3 (matching source id=3) is removed.
        sql """
            merge into merge_target t
            using merge_source s
            on t.id = s.id and s.id = 3
            when matched then delete
        """
        order_qt_merge_after_matched_delete """select id, note from merge_target"""

        // ==================== UPDATE/MERGE stay rejected on append-only shapes ====================
        // They need deletion-vector marks PLUS appended replacement rows in one write.

        test {
            sql """update dv_delete set note = 'x' where id = 4"""
            exception "Only DELETE is supported"
        }
        test {
            sql """update plain_append set note = 'x' where id = 1"""
            exception "Only DELETE is supported"
        }
        order_qt_plain_append_still_untouched """select id, note from plain_append"""

        // ==================== INSERT OVERWRITE: full-table and static-partition ====================
        // The static PARTITION(dt=...) form must materialize the clause literal into the partition
        // column (BindSink routes a materializing connector through the full-schema projection), so
        // the write lands in — and replaces — exactly the named partition. Without the materialize
        // step the row would carry a NULL partition value and Paimon's overwrite commit would reject
        // it as __DEFAULT_PARTITION__. Full-table OVERWRITE keeps its documented static semantics:
        // it replaces EVERY partition.

        sql """drop table if exists ow_part"""
        sql """create table ow_part (id int, v string, dt date) engine=paimon partition by list (dt) ()"""
        sql """insert into ow_part values (1, 'd1-a', date '2026-01-01'), (2, 'd1-b', date '2026-01-01'), (3, 'd2-a', date '2026-01-02')"""
        order_qt_ow_seed """select id, v, cast(dt as string) from ow_part"""

        // Static-partition overwrite: replaces ONLY dt=2026-01-01; dt=2026-01-02 must survive.
        sql """insert overwrite table ow_part partition(dt='2026-01-01') select 10, 'd1-new'"""
        order_qt_ow_static """select id, v, cast(dt as string) from ow_part"""

        // Repeating the same static overwrite is idempotent — the rerun replaces its own output.
        sql """insert overwrite table ow_part partition(dt='2026-01-01') select 11, 'd1-rerun'"""
        order_qt_ow_static_rerun """select id, v, cast(dt as string) from ow_part"""

        // Full-table overwrite wipes ALL partitions and leaves only the fresh rows.
        sql """insert overwrite table ow_part select 20, 'full', date '2026-01-03'"""
        order_qt_ow_full """select id, v, cast(dt as string) from ow_part"""
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
