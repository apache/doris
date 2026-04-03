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

package org.apache.doris.datasource.systable;

import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergSysExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class IcebergSysTableResolverTest {
    @Mocked
    private IcebergExternalCatalog catalog;
    @Mocked
    private IcebergExternalDatabase db;

    @Test
    public void testSupportedSysTablesExcludePositionDeletes() {
        Assertions.assertFalse(IcebergSysTable.SUPPORTED_SYS_TABLES.containsKey(IcebergSysTable.POSITION_DELETES));
    }

    @Test
    public void testResolveForPlanAndDescribeUseNativePath() throws Exception {
        IcebergExternalTable sourceTable = newIcebergTable();

        Optional<SysTableResolver.SysTablePlan> plan = SysTableResolver.resolveForPlan(
                sourceTable, "test_ctl", "test_db", "tbl$snapshots");
        Assertions.assertTrue(plan.isPresent());
        Assertions.assertTrue(plan.get().isNative());
        Assertions.assertTrue(plan.get().getSysExternalTable() instanceof IcebergSysExternalTable);
        Assertions.assertEquals("tbl$snapshots", plan.get().getSysExternalTable().getName());

        Optional<SysTableResolver.SysTableDescribe> describe = SysTableResolver.resolveForDescribe(
                sourceTable, "test_ctl", "test_db", "tbl$snapshots");
        Assertions.assertTrue(describe.isPresent());
        Assertions.assertTrue(describe.get().isNative());
        Assertions.assertTrue(describe.get().getSysExternalTable() instanceof IcebergSysExternalTable);
        Assertions.assertEquals("tbl$snapshots", describe.get().getSysExternalTable().getName());
    }

    @Test
    public void testPositionDeletesKeepsUnsupportedError() throws Exception {
        IcebergExternalTable sourceTable = newIcebergTable();
        Assertions.assertThrows(AnalysisException.class, () ->
                SysTableResolver.resolveForPlan(sourceTable, "test_ctl", "test_db", "tbl$position_deletes"));
    }

    private IcebergExternalTable newIcebergTable() throws Exception {
        new Expectations() {
            {
                catalog.getId();
                minTimes = 0;
                result = 1L;

                db.getFullName();
                minTimes = 0;
                result = "test_db";

                db.getRemoteName();
                minTimes = 0;
                result = "test_db";

                catalog.getDbOrAnalysisException("test_db");
                minTimes = 0;
                result = db;

                db.getId();
                minTimes = 0;
                result = 2L;

                db.makeSureInitialized();
                minTimes = 0;
            }
        };
        return new IcebergExternalTable(3L, "tbl", "tbl", catalog, db);
    }
}
