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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.doris.FeServiceClient;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.doris.RemoteDorisExternalDatabase;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.thrift.TCommitRemoteTxnRequest;
import org.apache.doris.thrift.TCommitRemoteTxnResult;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnWriteSchemaSnapshot;
import org.apache.doris.transaction.TxnWriteSchemaSnapshotCodec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class RemoteOlapInsertExecutorTest {
    @Test
    void commitUsesDestinationTableIdAfterLocalRemapping() throws Exception {
        Column key = new Column("k", PrimitiveType.INT);
        key.setIsKey(true);
        List<Column> columns = Collections.singletonList(key);
        OlapTable destination = new OlapTable(101L, "target", columns, KeysType.DUP_KEYS, null, null);
        destination.setBaseIndexId(202L);
        destination.setIndexMeta(202L, "target", columns, 3, 17, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        RemoteOlapTable remote = RemoteOlapTable.fromOlapTable(destination);
        // Match RemoteDorisExternalTable.loadDorisOlapTable's local ID rewrite.
        remote.setId(901L);
        Assertions.assertEquals(101L, remote.getRemoteTableId());
        RemoteDorisExternalCatalog catalog = Mockito.mock(RemoteDorisExternalCatalog.class);
        RemoteDorisExternalDatabase database = Mockito.mock(RemoteDorisExternalDatabase.class);
        FeServiceClient client = Mockito.mock(FeServiceClient.class);
        remote.setCatalog(catalog);
        remote.setDatabase(database);
        Mockito.when(catalog.getFeServiceClient()).thenReturn(client);
        Mockito.when(catalog.getName()).thenReturn("remote_catalog");
        Mockito.when(database.getId()).thenReturn(42L);
        Mockito.when(database.getFullName()).thenReturn("remote_db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getCommitInfos()).thenReturn(Collections.emptyList());
        ConnectContext ctx = new ConnectContext();
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        Mockito.when(factory.createCoordinator(Mockito.eq(ctx), Mockito.any(), Mockito.any(), Mockito.anyLong()))
                .thenReturn(coordinator);
        Mockito.when(client.commitRemoteTxn(Mockito.any())).thenAnswer(invocation -> {
            TCommitRemoteTxnRequest request = invocation.getArgument(0);
            // Validate the actual outgoing request with the destination's identity checks.
            TxnWriteSchemaSnapshot decoded = TxnWriteSchemaSnapshotCodec.fromThrift(
                    request.getWriteSchemas().get(0), destination.getId(), destination.getBaseIndexId());
            Assertions.assertEquals(Collections.singletonMap(202L, 3), decoded.getIndexSchemaVersions());
            return new TCommitRemoteTxnResult().setStatus(new TStatus(TStatusCode.OK)).setTxnStatus(true);
        });
        try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class)) {
            factories.when(EnvFactory::getInstance).thenReturn(factory);
            RemoteOlapInsertExecutor executor = new RemoteOlapInsertExecutor(ctx, remote, "label",
                    Mockito.mock(NereidsPlanner.class), Optional.empty(), false, 1L);
            executor.txnId = 7L;
            TxnWriteSchemaSnapshot localSnapshot = TxnWriteSchemaSnapshot.capture(remote, false);
            Field snapshotField = RemoteOlapInsertExecutor.class.getDeclaredField("writeSchemaSnapshot");
            snapshotField.setAccessible(true);
            snapshotField.set(executor, localSnapshot);
            executor.onComplete();
            Mockito.verify(client).commitRemoteTxn(Mockito.any());
            Assertions.assertEquals(TransactionStatus.VISIBLE, executor.txnStatus);
            Assertions.assertEquals(901L, localSnapshot.getTableId());
            Assertions.assertEquals(901L, remote.getId());
            Assertions.assertEquals(101L, destination.getId());
        }
    }
}
