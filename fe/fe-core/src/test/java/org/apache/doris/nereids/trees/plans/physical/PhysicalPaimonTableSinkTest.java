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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class PhysicalPaimonTableSinkTest {

    @Test
    void testFixedAppendCompactionRequiresSingleWriter() {
        Assertions.assertTrue(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.HASH_FIXED, Collections.emptyList(), Collections.emptyMap())));
        Assertions.assertFalse(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.HASH_FIXED, Collections.emptyList(),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"))));
    }

    @Test
    void testFixedPrimaryKeyAndDynamicModesRequireSingleWriter() {
        Assertions.assertTrue(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.HASH_FIXED, Collections.singletonList("id"),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"))));
        Assertions.assertTrue(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.HASH_DYNAMIC, Collections.singletonList("id"),
                        Collections.emptyMap())));
        Assertions.assertTrue(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.KEY_DYNAMIC, Collections.singletonList("id"),
                        Collections.emptyMap())));
    }

    private static FileStoreTable table(
            BucketMode bucketMode, List<String> primaryKeys, Map<String, String> options) {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.bucketMode()).thenReturn(bucketMode);
        Mockito.when(table.primaryKeys()).thenReturn(primaryKeys);
        Mockito.when(table.options()).thenReturn(options);
        return table;
    }
}
