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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PruneFileScanPartitionTest {
    @Test
    public void testPartitionColumnsUseRelationSnapshot() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        MvccSnapshot relationSnapshot = Mockito.mock(MvccSnapshot.class);
        List<Column> partitionColumns = Collections.singletonList(new Column("old_partition", Type.INT, true));
        Mockito.when(scan.getRelationSnapshot()).thenReturn(Optional.of(relationSnapshot));
        Mockito.when(table.getPartitionColumns(Optional.of(relationSnapshot))).thenReturn(partitionColumns);

        Assertions.assertSame(partitionColumns,
                PruneFileScanPartition.getPartitionColumnsForScan(table, scan));
        Mockito.verify(table).getPartitionColumns(Optional.of(relationSnapshot));
    }
}
