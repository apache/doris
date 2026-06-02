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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;

import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class IcebergDmlCommandUtilsTest {

    @Test
    public void testDefaultModesRejectCopyOnWriteOperations() {
        IcebergExternalTable table = mockIcebergExternalTable(new HashMap<>());

        assertCopyOnWriteException(() -> IcebergDmlCommandUtils.checkDeleteMode(table),
                "DELETE", TableProperties.DELETE_MODE);
        assertCopyOnWriteException(() -> IcebergDmlCommandUtils.checkUpdateMode(table),
                "UPDATE", TableProperties.UPDATE_MODE);
        assertCopyOnWriteException(() -> IcebergDmlCommandUtils.checkMergeMode(table),
                "MERGE INTO", TableProperties.MERGE_MODE);
    }

    @Test
    public void testExplicitCopyOnWriteModeRejectsOperation() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
        properties.put(TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
        properties.put(TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
        IcebergExternalTable table = mockIcebergExternalTable(properties);

        assertCopyOnWriteException(() -> IcebergDmlCommandUtils.checkDeleteMode(table),
                "DELETE", TableProperties.DELETE_MODE);
        assertCopyOnWriteException(() -> IcebergDmlCommandUtils.checkUpdateMode(table),
                "UPDATE", TableProperties.UPDATE_MODE);
        assertCopyOnWriteException(() -> IcebergDmlCommandUtils.checkMergeMode(table),
                "MERGE INTO", TableProperties.MERGE_MODE);
    }

    @Test
    public void testMergeOnReadModeAllowsOperation() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        properties.put(TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        properties.put(TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        IcebergExternalTable table = mockIcebergExternalTable(properties);

        Assertions.assertDoesNotThrow(() -> IcebergDmlCommandUtils.checkDeleteMode(table));
        Assertions.assertDoesNotThrow(() -> IcebergDmlCommandUtils.checkUpdateMode(table));
        Assertions.assertDoesNotThrow(() -> IcebergDmlCommandUtils.checkMergeMode(table));
    }

    private static void assertCopyOnWriteException(Runnable action, String operation, String property) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, action::run);
        Assertions.assertTrue(exception.getMessage().contains(operation));
        Assertions.assertTrue(exception.getMessage().contains("copy-on-write"));
        Assertions.assertTrue(exception.getMessage().contains(property));
    }

    private static IcebergExternalTable mockIcebergExternalTable(Map<String, String> properties) {
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.properties()).thenReturn(properties);

        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.getIcebergTable()).thenReturn(icebergTable);
        return table;
    }
}
