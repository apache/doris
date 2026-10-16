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

package org.apache.doris.jni.spi.vec;

import org.apache.doris.jni.spi.utils.OffHeap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;

/**
 * A VARBINARY column keeps a [length, address] record per row in an off-heap buffer of its own, apart from the row
 * bytes the records point to. A scanner closes its table after every batch, so a close that leaves the buffer behind
 * loses it once per batch and column.
 */
class VectorTableVarbinaryCloseTest {

    @Test
    void closeFreesTheRowRecordsOfAVarbinaryColumn() {
        OffHeap.setTesting();
        VectorTable table = VectorTable.createWritableTable(
                new ColumnType[] {ColumnType.parseType("b", "varbinary(16)")}, new String[] {"b"}, 4);
        table.getColumn(0).appendVarbinary(
                new byte[][] {"abc".getBytes(StandardCharsets.UTF_8), null, new byte[0]}, true);
        long records = table.getColumn(0).dataAddress();
        Assertions.assertNotEquals(0L, records);

        try (MockedStatic<OffHeap> offHeap = Mockito.mockStatic(OffHeap.class, Mockito.CALLS_REAL_METHODS)) {
            table.close();
            offHeap.verify(() -> OffHeap.freeMemory(records));
        }
    }
}
