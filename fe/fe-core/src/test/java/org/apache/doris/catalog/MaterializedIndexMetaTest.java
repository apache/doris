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

package org.apache.doris.catalog;

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MaterializedIndexMetaTest {

    @Test
    public void testDeserializeRepairsStaleMaxColUniqueId() throws Exception {
        MaterializedIndexMeta meta = createMeta();
        meta.setMaxColUniqueId(Column.COLUMN_UNIQUE_ID_INIT_VALUE);

        String json = GsonUtils.GSON.toJson(meta);
        MaterializedIndexMeta replayedMeta = GsonUtils.GSON.fromJson(json, MaterializedIndexMeta.class);

        Assertions.assertEquals(7, replayedMeta.getMaxColUniqueId());
        replayedMeta.setSchema(Lists.newArrayList(replayedMeta.getSchema().get(0)));
        Assertions.assertEquals(8, replayedMeta.incAndGetMaxColUniqueId());
    }

    @Test
    public void testDeserializePreservesHigherMaxColUniqueId() throws Exception {
        MaterializedIndexMeta meta = createMeta();
        meta.setMaxColUniqueId(11);

        String json = GsonUtils.GSON.toJson(meta);
        MaterializedIndexMeta replayedMeta = GsonUtils.GSON.fromJson(json, MaterializedIndexMeta.class);

        replayedMeta.setSchema(Lists.newArrayList(replayedMeta.getSchema().get(0)));
        Assertions.assertEquals(11, replayedMeta.getMaxColUniqueId());
        Assertions.assertEquals(12, replayedMeta.incAndGetMaxColUniqueId());
    }

    private MaterializedIndexMeta createMeta() {
        Column keyColumn = new Column("k", PrimitiveType.INT);
        keyColumn.setUniqueId(0);
        Column valueColumn = new Column("v", PrimitiveType.INT);
        valueColumn.setUniqueId(7);
        return new MaterializedIndexMeta(
                1L, Lists.newArrayList(keyColumn, valueColumn), 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null);
    }
}
