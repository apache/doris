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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class IcebergSysTableResolverTest {
    private IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
    private IcebergExternalDatabase db = Mockito.mock(IcebergExternalDatabase.class);

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
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(db.getFullName()).thenReturn("test_db");
        Mockito.when(db.getRemoteName()).thenReturn("test_db");
        Mockito.doReturn(db).when(catalog).getDbOrAnalysisException("test_db");
        Mockito.when(db.getId()).thenReturn(2L);
        return new IcebergExternalTable(3L, "tbl", "tbl", catalog, db);
    }
}
