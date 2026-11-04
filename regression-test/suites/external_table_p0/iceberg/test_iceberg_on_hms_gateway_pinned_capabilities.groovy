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

// An iceberg table stored in HMS is reachable through a dedicated iceberg catalog ('type'='iceberg',
// 'iceberg.catalog.type'='hms') and through the heterogeneous HMS gateway catalog ('type'='hms'). The gateway
// passes a fixed set of per-table capabilities on from its embedded iceberg sibling to such a table (Top-N lazy
// materialization, nested column pruning, field-id access paths, write-only defaults, ...), and fe-core reads
// them from the schema the statement uses. A statement that reads an iceberg table pins its latest snapshot and
// uses the schema retained with that pin, which the gateway builds through the snapshot overload of
// getTableSchema, as it does for explicit time travel. That overload used to return the sibling's schema without
// the inherited capabilities, so in every such statement an iceberg-on-HMS table planned as if it had none.
//
// The suite builds two identical tables through the dedicated catalog, then drives the same statements through
// the dedicated catalog against one and through the gateway against the other. The plan and error checks are the
// same for both, and the .out file holds the results of both under paired tags, which must be identical.
suite("test_iceberg_on_hms_gateway_pinned_capabilities", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hive test.")
        return
    }

    String hmsPort = context.config.otherConfigs.get("hive3HmsPort")
    String hdfsPort = context.config.otherConfigs.get("hive3HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String icebergCatalog = "test_iceberg_on_hms_pinned_caps_iceberg"
    String gatewayCatalog = "test_iceberg_on_hms_pinned_caps_hms"
    String dbName = "test_iceberg_on_hms_pinned_caps_db"

    sql """drop catalog if exists ${icebergCatalog}"""
    sql """create catalog ${icebergCatalog} properties (
        'type'='iceberg',
        'iceberg.catalog.type'='hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
        'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}'
    );"""
    sql """drop catalog if exists ${gatewayCatalog}"""
    sql """create catalog ${gatewayCatalog} properties (
        'type'='hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
        'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}'
    );"""

    // Built through the dedicated catalog for both runs. The first two rows carry d only as the initial default
    // 42, and their data files still name the renamed struct field s.a2 as a. Returns the first snapshot.
    def createTable = { String tableName ->
        sql """switch ${icebergCatalog}"""
        sql """create database if not exists ${dbName}"""
        sql """use ${dbName}"""
        sql """drop table if exists ${tableName}"""
        sql """create table ${tableName} (id INT, c INT, s STRUCT<a:INT, b:STRING>)
               properties ('format-version' = '3')"""
        sql """insert into ${tableName} values (0, 0, named_struct('a', 1, 'b', 'x')), (1, 1, named_struct('a', 2, 'b', 'y'))"""
        sql """alter table ${tableName} add column d INT DEFAULT 42"""
        sql """alter table ${tableName} rename column s.a TO a2"""
        sql """insert into ${tableName} (id, c, s, d) values (2, 2, named_struct('a2', 3, 'b', 'z'), 7)"""
        List<List<Object>> snapshots = sql """select snapshot_id from ${tableName}\$snapshots order by committed_at"""
        return snapshots[0][0]
    }

    def runProbe = { String catalogName, String tableName, String tag ->
        def firstSnapshot = createTable(tableName)
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""

        // Top-N lazy materialization, under the latest-snapshot pin and under explicit time travel.
        explain {
            sql """select * from ${tableName} order by id limit 1"""
            contains "VMaterializeNode"
            contains "row_ids: [__DORIS_GLOBAL_ROWID_COL__${tableName}]"
        }
        explain {
            sql """select * from ${tableName} for version as of ${firstSnapshot} order by id limit 1"""
            contains "VMaterializeNode"
        }
        // Nested column pruning. The pruned read of the renamed field needs the field-id access path: by name,
        // the old files would not have s.a2.
        explain {
            sql """select struct_element(s, 'a2') from ${tableName}"""
            contains "all access paths: [s.a2]"
        }
        "order_qt_${tag}_renamed_nested" """select id, struct_element(s, 'a2'), struct_element(s, 'b') from ${tableName}"""
        "qt_${tag}_renamed_nested_topn" """select id, s from ${tableName} order by id limit 2"""

        // Write-only defaults: DEFAULT(d) is the write-default 42 in the conditions of UPDATE and MERGE ...
        sql """update ${tableName} set c = 100 where d = DEFAULT(d)"""
        "order_qt_${tag}_update_where_default" """select id, c, d from ${tableName}"""
        sql """merge into ${tableName} t using (select 0 as id) src on t.id = src.id and t.d = DEFAULT(t.d)
               when matched then update set c = 300"""
        "order_qt_${tag}_merge_on_default" """select id, c, d from ${tableName}"""
        // ... and it is refused where the statement writes no row.
        test {
            sql """delete from ${tableName} where d = DEFAULT(d)"""
            exception "DEFAULT(d) cannot be used in a DELETE"
        }
        test {
            sql """select DEFAULT(d) from ${tableName}"""
            exception "DEFAULT(d) must reference a column of the table that this statement writes"
        }
    }

    runProbe(icebergCatalog, "pinned_caps_iceberg", "iceberg")
    runProbe(gatewayCatalog, "pinned_caps_gateway", "gateway")
}
