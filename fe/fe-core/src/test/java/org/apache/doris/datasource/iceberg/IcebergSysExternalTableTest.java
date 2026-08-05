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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.MetadataTableType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class IcebergSysExternalTableTest {
    @Test
    public void testStaticMetadataTablesDoNotSupportSnapshotSelection() {
        IcebergExternalTable sourceTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(sourceTable.getId()).thenReturn(1L);
        Mockito.when(sourceTable.getName()).thenReturn("table");
        Mockito.when(sourceTable.getRemoteName()).thenReturn("table");
        Mockito.when(sourceTable.getCatalog()).thenReturn(Mockito.mock(IcebergExternalCatalog.class));
        Mockito.when(sourceTable.getDatabase()).thenReturn(Mockito.mock(IcebergExternalDatabase.class));

        for (MetadataTableType type : new MetadataTableType[] {
                MetadataTableType.HISTORY,
                MetadataTableType.SNAPSHOTS,
                MetadataTableType.REFS,
                MetadataTableType.METADATA_LOG_ENTRIES}) {
            IcebergSysExternalTable table = new IcebergSysExternalTable(sourceTable, type.name());
            Assertions.assertFalse(table.supportsSnapshotSelection(), type.name());
        }

        IcebergSysExternalTable dataFiles = new IcebergSysExternalTable(
                sourceTable, MetadataTableType.DATA_FILES.name());
        Assertions.assertTrue(dataFiles.supportsSnapshotSelection());
    }
}
