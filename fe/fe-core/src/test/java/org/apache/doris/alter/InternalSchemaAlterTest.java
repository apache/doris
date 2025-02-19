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

package org.apache.doris.alter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.ColumnNullableType;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Map;

public class InternalSchemaAlterTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        Config.allow_replica_on_same_host = true;
        FeConstants.runningUnitTest = true;
        InternalSchemaInitializer.createDb();
        InternalSchemaInitializer.createTbl();
    }

    @Test
    public void testModifyTblReplicaCount() throws AnalysisException {
        Database db = Env.getCurrentEnv().getCatalogMgr()
                .getInternalCatalog().getDbNullable(FeConstants.INTERNAL_DB_NAME);

        InternalSchemaInitializer.modifyTblReplicaCount(db, StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        InternalSchemaInitializer.modifyTblReplicaCount(db, AuditLoader.AUDIT_LOG_TABLE);

        checkReplicationNum(db, StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        checkReplicationNum(db, AuditLoader.AUDIT_LOG_TABLE);
    }

    private void checkReplicationNum(Database db, String tblName) throws AnalysisException {
        OlapTable olapTable = db.getOlapTableOrAnalysisException(tblName);
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        Assertions.assertEquals((short) 3, olapTable.getTableProperty().getReplicaAllocation().getTotalReplicaNum(),
                tblName);
        for (Partition partition : olapTable.getPartitions()) {
            Assertions.assertEquals((short) 3,
                    partitionInfo.getReplicaAllocation(partition.getId()).getTotalReplicaNum());
        }
    }

    @Test
    public void testCheckAuditLogTable() throws AnalysisException {
        Database db = Env.getCurrentEnv().getCatalogMgr()
                .getInternalCatalog().getDbNullable(FeConstants.INTERNAL_DB_NAME);
        Assertions.assertNotNull(db);
        OlapTable table = db.getOlapTableOrAnalysisException(AuditLoader.AUDIT_LOG_TABLE);
        Assertions.assertNotNull(table);
        for (ColumnDef def : InternalSchema.AUDIT_SCHEMA) {
            Assertions.assertNotNull(table.getColumn(def.getName()));
        }
    }

    @Test
    public void testAuditLogTableSchemaUpgrade() throws Exception {
        // create a database and old version audit log table
        Database db = Env.getCurrentEnv().getCatalogMgr()
                .getInternalCatalog().getDbNullable(FeConstants.INTERNAL_DB_NAME);
        Assertions.assertNotNull(db);

        // drop existing table if exists
        try {
            Env.getCurrentEnv().dropTable(new DropTableStmt(true,
                    new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, FeConstants.INTERNAL_DB_NAME,
                            AuditLoader.AUDIT_LOG_TABLE), true));
        } catch (Exception e) {
            // ignore if table doesn't exist
        }

        // create old version schema (without new columns)
        ArrayList<ColumnDef> oldSchema = Lists.newArrayList();
        oldSchema.add(new ColumnDef("query_id", TypeDef.createVarchar(48),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("time", TypeDef.createDatetimeV2(3),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("client_ip", TypeDef.createVarchar(128),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("user", TypeDef.createVarchar(128),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("catalog", TypeDef.createVarchar(128),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("db", TypeDef.createVarchar(128),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("state", TypeDef.createVarchar(128),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("error_code", TypeDef.create(PrimitiveType.INT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("error_message", TypeDef.create(PrimitiveType.STRING),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("query_time", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("scan_bytes", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("scan_rows", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("return_rows", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("shuffle_send_rows", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("shuffle_send_bytes", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("stmt_id", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("stmt_type", TypeDef.createVarchar(48),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("is_query", TypeDef.create(PrimitiveType.TINYINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("frontend_ip", TypeDef.createVarchar(128),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("cpu_time_ms", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("sql_hash", TypeDef.createVarchar(128),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("sql_digest", TypeDef.createVarchar(128),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("peak_memory_bytes", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("workload_group", TypeDef.create(PrimitiveType.STRING),
                ColumnNullableType.NULLABLE));
        oldSchema.add(new ColumnDef("stmt", TypeDef.create(PrimitiveType.STRING),
                ColumnNullableType.NULLABLE));

        // create old version table
        TableName tableName = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME,
                FeConstants.INTERNAL_DB_NAME, AuditLoader.AUDIT_LOG_TABLE);
        ArrayList<String> dupKeys = Lists.newArrayList("query_id", "time", "client_ip");
        KeysDesc keysDesc = new KeysDesc(KeysType.DUP_KEYS, dupKeys);
        PartitionDesc partitionDesc = new RangePartitionDesc(Lists.newArrayList("time"),
                Lists.newArrayList());
        DistributionDesc distributionDesc = new HashDistributionDesc(2,
                Lists.newArrayList("query_id"));
        Map<String, String> properties = Maps.newHashMap();
        properties.put("replication_num", "1");

        CreateTableStmt createTableStmt = new CreateTableStmt(true, false, tableName, oldSchema,
                "olap", keysDesc, partitionDesc, distributionDesc, properties, null,
                        "Old version audit table for test", null);
        Env.getCurrentEnv().createTable(createTableStmt);

        // run schema upgrade
        InternalSchemaInitializer initializer = new InternalSchemaInitializer();
        initializer.run();

        // verify schema
        OlapTable newTable = db.getOlapTableOrAnalysisException(AuditLoader.AUDIT_LOG_TABLE);
        List<Column> newSchema = newTable.getFullSchema();

        // verify new columns are added in correct position
        int shuffleSendBytesPos = 0;
        int localStoragePos = -1;
        int remoteStoragePos = -1;
        int stmtIdPos = -1;

        for (int i = 0; i < newSchema.size(); i++) {
            Column col = newSchema.get(i);
            switch (col.getName()) {
                case "shuffle_send_bytes":
                    shuffleSendBytesPos = i;
                    break;
                case "scan_bytes_from_local_storage":
                    localStoragePos = i;
                    break;
                case "scan_bytes_from_remote_storage":
                    remoteStoragePos = i;
                    break;
                case "stmt_id":
                    stmtIdPos = i;
                    break;
            }
        }

        // verify positions
        Assertions.assertTrue(shuffleSendBytesPos >= 0);
        Assertions.assertTrue(localStoragePos >= 0);
        Assertions.assertTrue(remoteStoragePos >= 0);
        Assertions.assertTrue(stmtIdPos >= 0);

        // verify order
        Assertions.assertTrue(shuffleSendBytesPos < localStoragePos);
        Assertions.assertTrue(localStoragePos < remoteStoragePos);
        Assertions.assertTrue(remoteStoragePos < stmtIdPos);

        // verify column types
        Column localStorageCol = newSchema.get(localStoragePos);
        Column remoteStorageCol = newSchema.get(remoteStoragePos);
        Assertions.assertEquals(PrimitiveType.BIGINT, localStorageCol.getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.BIGINT, remoteStorageCol.getType().getPrimitiveType());
        Assertions.assertTrue(localStorageCol.isAllowNull());
        Assertions.assertTrue(remoteStorageCol.isAllowNull());
    }
}
