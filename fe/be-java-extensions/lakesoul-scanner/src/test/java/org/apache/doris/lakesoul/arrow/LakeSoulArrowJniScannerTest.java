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

package org.apache.doris.lakesoul.arrow;

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType;

import org.junit.Assert;
import org.junit.Test;

public class LakeSoulArrowJniScannerTest {
    @Test
    public void testBeMetaAddressSkipsJavaReadableConstFlag() {
        OffHeap.setTesting();
        long javaMetaAddress = OffHeap.allocateMemory(4L * Long.BYTES);
        long nullMapAddress = 100;
        long dataAddress = 200;
        OffHeap.putLong(null, javaMetaAddress, 2);
        OffHeap.putLong(null, javaMetaAddress + Long.BYTES, 0);
        OffHeap.putLong(null, javaMetaAddress + 2L * Long.BYTES, nullMapAddress);
        OffHeap.putLong(null, javaMetaAddress + 3L * Long.BYTES, dataAddress);

        long beMetaAddress = LakeSoulArrowJniScanner.buildBeMetaAddress(
                new ColumnType[] {ColumnType.parseType("c1", "int")}, javaMetaAddress, 2);
        try {
            Assert.assertEquals(2, OffHeap.getLong(null, beMetaAddress));
            Assert.assertEquals(nullMapAddress, OffHeap.getLong(null, beMetaAddress + Long.BYTES));
            Assert.assertEquals(dataAddress, OffHeap.getLong(null, beMetaAddress + 2L * Long.BYTES));
        } finally {
            OffHeap.freeMemory(javaMetaAddress);
            OffHeap.freeMemory(beMetaAddress);
        }
    }
}
