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

package org.apache.doris.common.jni;

import org.apache.doris.common.jni.utils.JavaUdfDataType;
import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorTable;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class UuidVectorTest {
    @Test
    public void testUuidBatchGrowthNestedNullsAndReuse() {
        OffHeap.setTesting();
        Assert.assertTrue(JavaUdfDataType.getCandidateTypes(UUID.class).contains(JavaUdfDataType.UUID));
        ColumnType[] types = {ColumnType.parseType("u", "uuid"),
                ColumnType.parseType("a", "array<uuid>")};
        String[] fields = {"u", "a"};
        UUID[] values = new UUID[4097];
        @SuppressWarnings("unchecked")
        List<Object>[] arrays = (List<Object>[]) new List<?>[values.length];
        for (int i = 0; i < values.length; ++i) {
            values[i] = i % 3 == 0 ? null : new UUID(Long.MIN_VALUE + i, Long.MAX_VALUE - i);
            arrays[i] = i % 5 == 0 ? null : Arrays.asList(values[i], null);
        }
        VectorTable writable = VectorTable.createWritableTable(types, fields, 1);
        long nativeMeta = OffHeap.allocateMemory(10L * Long.BYTES);
        try {
            for (int batch = 0; batch < 2; ++batch) {
                writable.appendData(0, values, true);
                writable.appendData(1, arrays, true);
                // BE input metadata has a const flag before each column, including child columns.
                // Java output metadata omits these flags; adapt the two documented layouts.
                long javaMeta = writable.getMetaAddress();
                int[] offsets = {0, -1, 1, 2, -1, 3, 4, -1, 5, 6};
                for (int i = 0; i < offsets.length; ++i) {
                    OffHeap.putLong(null, nativeMeta + (long) i * Long.BYTES,
                            offsets[i] < 0 ? 0 : OffHeap.getLong(null, javaMeta + (long) offsets[i] * Long.BYTES));
                }
                VectorTable readable = VectorTable.createReadableTable(types, fields, nativeMeta);
                Object[][] restored = readable.getMaterializedData();
                Assert.assertArrayEquals(values, restored[0]);
                Assert.assertArrayEquals(arrays, restored[1]);
                Assert.assertArrayEquals(Arrays.copyOfRange(values, 1, 4097),
                        readable.getColumn(0).getUuidColumn(1, 4097));
                // The readable view borrows writable's buffers; writable owns their lifetime.
                writable.reset();
            }
        } finally {
            OffHeap.freeMemory(nativeMeta);
            writable.close();
        }
    }
}
