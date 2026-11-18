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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

class TxnWriteSchemaSnapshotTest {
    @Test
    void partialSchemaFreezesStringWidthsAndSharesThemWithCommit() {
        OlapTable table = Mockito.mock(OlapTable.class);
        MaterializedIndexMeta base = Mockito.mock(MaterializedIndexMeta.class);
        MaterializedIndexMeta rollup = Mockito.mock(MaterializedIndexMeta.class);
        Column column = new Column("value", ScalarType.createVarcharType(10));
        column.setUniqueId(7);
        Mockito.when(table.getId()).thenReturn(1L);
        Mockito.when(table.getBaseIndexId()).thenReturn(20L);
        Mockito.when(table.getIndexMetaByIndexId(20L)).thenReturn(base);
        Mockito.when(table.getIndexIdToMeta()).thenReturn(ImmutableMap.of(10L, rollup, 20L, base));
        Mockito.when(table.getIndexIdToMeta(true)).thenReturn(ImmutableMap.of(10L, rollup, 20L, base));
        Mockito.when(base.getSchema()).thenReturn(Collections.singletonList(column));
        Mockito.when(base.getSchemaVersion()).thenReturn(3);
        TxnWriteSchemaSnapshot snapshot = TxnWriteSchemaSnapshot.capture(table, true);
        TransactionState state = new TransactionState();
        state.registerWriteSchema(100, snapshot);
        Assertions.assertSame(snapshot.getPartialSchema(), state.getTxnSchema(1));
        Mockito.when(base.getSchemaVersion()).thenReturn(4);
        Assertions.assertTrue(state.checkSchemaCompatibility(table));
        ((ScalarType) column.getType()).setLength(20);
        Assertions.assertFalse(state.checkSchemaCompatibility(table));
        Mockito.when(base.getSchema()).thenReturn(Collections.emptyList());
        Assertions.assertTrue(state.checkSchemaCompatibility(table));
        Mockito.when(base.getSchema()).thenReturn(Collections.singletonList(column));
        Assertions.assertNull(state.getWriteSchema(100, 1).getPartialSchema());
        state.pruneAfterVisible();
        Assertions.assertNull(state.getTxnSchema(1));
        Assertions.assertTrue(state.checkSchemaCompatibility(table));
        Assertions.assertFalse(snapshot.getPartialSchema().isCompatibleWith(base));
        String json = GsonUtils.GSON.toJson(snapshot);
        Assertions.assertFalse(json.contains("partialSchema"));
        Assertions.assertEquals(snapshot, GsonUtils.GSON.fromJson(json, TxnWriteSchemaSnapshot.class));
    }

    @Test
    void ordinaryWritesFreezeVersionsAndPreserveFirstTableIndexes() {
        TransactionState state = new TransactionState();
        TxnWriteSchemaSnapshot first = new TxnWriteSchemaSnapshot(1, ImmutableMap.of(2L, 3),
                Collections.emptyMap(), false);
        state.registerWriteSchema(100, first);
        state.registerWriteSchema(101, new TxnWriteSchemaSnapshot(1, ImmutableMap.of(2L, 4, 5L, 1),
                Collections.emptyMap(), false));
        Assertions.assertEquals(Collections.singleton(2L), state.getLoadedTblIndexes().get(1L));
        Assertions.assertThrows(IllegalStateException.class, () -> state.registerWriteSchema(100,
                new TxnWriteSchemaSnapshot(1, ImmutableMap.of(2L, 4), Collections.emptyMap(), false)));
        state.setTransactionStatus(TransactionStatus.PRECOMMITTED);
        TransactionState replay = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(state), TransactionState.class);
        replay.registerWriteSchema(100, first);
        Assertions.assertThrows(IllegalStateException.class, () -> replay.registerWriteSchema(102, first));
        Assertions.assertEquals(first, replay.getWriteSchema(100, 1));
    }
}
