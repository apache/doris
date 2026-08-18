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

// Column schema evolution on Paimon 2.0 external tables: ALTER TABLE ADD / DROP / RENAME / MODIFY
// COLUMN, flat and nested, plus the read-compatibility guarantee that rows written under an older
// schema stay readable afterwards.
//
// A paimon ALTER bumps the schema id WITHOUT creating a data snapshot, so this suite asserts on BOTH
// the resulting column set (desc) and the data (select) — a schema change that lost rows, or a row
// read that ignored the new schema, would otherwise pass on one axis alone.
suite("test_paimon_schema_evolution",
        "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_schema_evolution"
    String dbName = "paimon_schema_evolution_db"
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

        // ==================== flat column evolution ====================

        sql """drop table if exists schema_evo"""
        sql """
            create table schema_evo (
                id int not null,
                score int,
                note string
            ) engine=paimon properties (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet'
            )
        """
        sql """insert into schema_evo values (1, 10, 'one'), (2, 20, 'two')"""
        order_qt_evo_before """select id, score, note from schema_evo"""

        // ADD COLUMN, appended (no position clause).
        sql """alter table schema_evo add column extra string"""
        qt_evo_after_add """desc schema_evo"""
        // The pre-ALTER rows must still be readable, with the new column reading NULL for them: this is
        // the schema-evolution read-compatibility guarantee, and it is what a naive "rewrite the schema"
        // implementation breaks.
        order_qt_evo_add_rows """select id, score, note, extra from schema_evo"""

        // ADD COLUMN FIRST / AFTER: position must be honored, not silently appended.
        sql """alter table schema_evo add column lead_col int first"""
        sql """alter table schema_evo add column mid_col int after score"""
        qt_evo_after_positions """desc schema_evo"""

        // New rows write under the new schema while old rows keep their (NULL) values.
        sql """insert into schema_evo values (100, 3, 30, 300, 'three', 'extra3')"""
        order_qt_evo_after_new_insert """select id, score, note, extra, lead_col, mid_col from schema_evo"""

        // RENAME COLUMN: the data must follow the rename, not be dropped and re-added as NULL.
        sql """alter table schema_evo rename column extra extra_renamed"""
        qt_evo_after_rename """desc schema_evo"""
        order_qt_evo_rename_rows """select id, extra_renamed from schema_evo"""

        // MODIFY COLUMN type widening (int -> bigint): existing values must survive the widening.
        sql """alter table schema_evo modify column score bigint"""
        qt_evo_after_widen """desc schema_evo"""
        order_qt_evo_widen_rows """select id, score from schema_evo"""

        // MODIFY COLUMN ... COMMENT routes through modifyColumnComment (the sole entrypoint), which must
        // change ONLY the comment.
        sql """alter table schema_evo modify column note string comment 'the note column'"""
        qt_evo_after_comment """desc schema_evo"""
        order_qt_evo_comment_rows """select id, note from schema_evo"""

        // DROP COLUMN: the remaining columns keep their values.
        sql """alter table schema_evo drop column mid_col"""
        qt_evo_after_drop """desc schema_evo"""
        order_qt_evo_drop_rows """select id, score, note, extra_renamed, lead_col from schema_evo"""

        // ==================== nested (dotted-path) column evolution ====================

        sql """drop table if exists schema_evo_nested"""
        sql """
            create table schema_evo_nested (
                id int not null,
                s struct<a:int, b:string>
            ) engine=paimon properties (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet'
            )
        """
        sql """insert into schema_evo_nested values (1, named_struct('a', 1, 'b', 'one'))"""
        order_qt_nested_before """select id, s from schema_evo_nested"""

        // Dotted paths address a field INSIDE the struct; the parent column must not be replaced.
        sql """alter table schema_evo_nested add column s.c int"""
        qt_nested_after_add """desc schema_evo_nested"""
        order_qt_nested_add_rows """select id, s from schema_evo_nested"""

        sql """alter table schema_evo_nested rename column s.b b_renamed"""
        qt_nested_after_rename """desc schema_evo_nested"""

        sql """alter table schema_evo_nested modify column s.a bigint"""
        qt_nested_after_modify """desc schema_evo_nested"""

        sql """alter table schema_evo_nested drop column s.c"""
        qt_nested_after_drop """desc schema_evo_nested"""
        order_qt_nested_final_rows """select id, s from schema_evo_nested"""

        // ==================== row-level DML after schema evolution ====================
        // Column evolution and row-level DML must COMPOSE: a DELETE/UPDATE issued after columns were
        // added, renamed and dropped has to address rows under the CURRENT schema. This is the
        // interaction most likely to break — the write path caches table metadata, and a stale schema
        // here would either fail to bind or write under the wrong column layout.

        sql """delete from schema_evo where id = 2"""
        order_qt_evo_after_delete """select id, score, note, extra_renamed, lead_col from schema_evo"""

        sql """update schema_evo set note = 'post-evolution' where id = 1"""
        order_qt_evo_after_update """select id, score, note, extra_renamed, lead_col from schema_evo"""
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
