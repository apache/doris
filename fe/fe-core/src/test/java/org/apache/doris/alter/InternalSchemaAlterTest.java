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

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.plugin.audit.AuditLoaderPlugin;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        InternalSchemaInitializer.modifyTblReplicaCount(db, AuditLoaderPlugin.AUDIT_LOG_TABLE);

        checkReplicationNum(db, StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        checkReplicationNum(db, AuditLoaderPlugin.AUDIT_LOG_TABLE);
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
        OlapTable table = db.getOlapTableOrAnalysisException(AuditLoaderPlugin.AUDIT_LOG_TABLE);
        Assertions.assertNotNull(table);
        for (ColumnDef def : InternalSchema.AUDIT_SCHEMA) {
            Assertions.assertNotNull(table.getColumn(def.getName()));
        }
    }
}
