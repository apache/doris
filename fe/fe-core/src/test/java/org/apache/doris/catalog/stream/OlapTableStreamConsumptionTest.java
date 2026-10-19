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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class OlapTableStreamConsumptionTest {

    @Test
    public void testUnitSelectorRunsAfterReleasingBaseTableLock() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition p1 = Mockito.mock(Partition.class);
        Partition p2 = Mockito.mock(Partition.class);
        AtomicBoolean tableLocked = new AtomicBoolean(false);
        Mockito.when(table.readLockIfExist()).thenAnswer(invocation -> {
            tableLocked.set(true);
            return true;
        });
        Mockito.doAnswer(invocation -> {
            tableLocked.set(false);
            return null;
        }).when(table).readUnlock();
        Mockito.when(table.getPartitions()).thenReturn(List.of(p1, p2));
        Mockito.when(p1.getName()).thenReturn("p1");
        Mockito.when(p2.getName()).thenReturn("p2");
        Mockito.when(p1.getId()).thenReturn(1L);
        Mockito.when(p2.getId()).thenReturn(2L);

        OlapTableStream stream = Mockito.spy(new OlapTableStream());
        Mockito.doReturn(table).when(stream).getBaseTableNullable();
        Deencapsulation.setField(stream, "partitionOffset", new HashMap<Long, Long>());
        Deencapsulation.setField(stream, "partitionConsumptionTime", new HashMap<Long, Long>());
        List<TRow> rows = new ArrayList<>();

        stream.fillTableStreamConsumptionInfo(rows, unit -> {
            Assertions.assertFalse(tableLocked.get());
            return false;
        });

        Assertions.assertTrue(rows.isEmpty());
    }
}
