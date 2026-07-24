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

package org.apache.doris.persist;

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.Assert;
import org.junit.Test;

public class PartitionPersistInfoTest {

    @Test
    public void testInvertedIndexStorageFormatSerialization() {
        PartitionPersistInfo partitionPersistInfo = new PartitionPersistInfo(1L, 2L, null, null, null,
                null, null, false, false, true, TInvertedIndexFileStorageFormat.V3);
        PartitionPersistInfo copiedPartitionPersistInfo = GsonUtils.GSON.fromJson(partitionPersistInfo.toJson(),
                PartitionPersistInfo.class);
        Assert.assertEquals(TInvertedIndexFileStorageFormat.V3,
                copiedPartitionPersistInfo.getInvertedIndexFileStorageFormat());

        PartitionPersistInfo oldPartitionPersistInfo = GsonUtils.GSON.fromJson("{\"dbId\":1,\"tableId\":2}",
                PartitionPersistInfo.class);
        Assert.assertNull(oldPartitionPersistInfo.getInvertedIndexFileStorageFormat());
    }
}
