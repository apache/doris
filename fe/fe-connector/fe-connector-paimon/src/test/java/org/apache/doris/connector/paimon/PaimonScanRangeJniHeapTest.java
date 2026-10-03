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

package org.apache.doris.connector.paimon;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The JVM heap a JNI split declares reaches BE's JNI heap gate as {@code TFileRangeDesc.jni_heap_bytes},
 * and nothing else does: a split that declared nothing (the statement did not set
 * {@code enable_jni_heap_admission}) leaves it unset, so that BE opens its reader without waiting.
 */
public class PaimonScanRangeJniHeapTest {

    private static TFileRangeDesc populate(PaimonScanRange range) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(new TTableFormatFileDesc(), rangeDesc);
        return rangeDesc;
    }

    @Test
    public void jniSplitHandsTheHeapItDeclaredToBe() {
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("parquet")
                .paimonSplit("serialized-split")
                .jniHeapBytes(150L << 20)
                .build();

        TFileRangeDesc desc = populate(range);
        Assertions.assertTrue(desc.isSetJniHeapBytes());
        Assertions.assertEquals(150L << 20, desc.getJniHeapBytes());
    }

    @Test
    public void jniSplitThatDeclaredNothingLeavesTheGateOut() {
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("parquet")
                .paimonSplit("serialized-split")
                .build();

        Assertions.assertFalse(populate(range).isSetJniHeapBytes());
        Assertions.assertFalse(range.getProperties().containsKey("paimon.jni_heap_bytes"));
    }

    @Test
    public void nativeSplitNeverDeclaresHeap() {
        // BE reads a native split in C++; there is no Java scanner to admit.
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("parquet")
                .path("s3://bkt/a/part-0.parquet")
                .jniHeapBytes(150L << 20)
                .build();

        Assertions.assertFalse(populate(range).isSetJniHeapBytes());
        Assertions.assertFalse(range.getProperties().containsKey("paimon.jni_heap_bytes"));
    }
}
