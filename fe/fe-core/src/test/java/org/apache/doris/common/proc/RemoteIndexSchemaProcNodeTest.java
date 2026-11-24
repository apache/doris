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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class RemoteIndexSchemaProcNodeTest {
    @Test
    void repeatedFetchDoesNotAccumulateRemoteColumns() {
        List<Column> schema = new ArrayList<>(ImmutableList.of(new Column("v", Type.VARIANT)));
        ProcResult base = IndexSchemaProcNode.createResult(schema, null, ImmutableList.of());
        RemoteIndexSchemaProcNode node = new RemoteIndexSchemaProcNode(base, null,
                () -> ImmutableList.of(new Column("v.key", Type.INT)));
        Assertions.assertEquals(2, node.fetchResult().getRows().size());
        Assertions.assertEquals(2, node.fetchResult().getRows().size());
        Assertions.assertEquals(1, schema.size());
        Assertions.assertEquals(1, base.getRows().size());
    }

    @Test
    void remoteWorkStartsOnlyAfterSnapshotIsConsumed() throws Exception {
        CountDownLatch remoteStarted = new CountDownLatch(1);
        CountDownLatch releaseRemote = new CountDownLatch(1);
        ProcResult base = IndexSchemaProcNode.createResult(ImmutableList.of(new Column("v", Type.VARIANT)), null, ImmutableList.of());
        RemoteIndexSchemaProcNode node = new RemoteIndexSchemaProcNode(base, null, () -> {
            remoteStarted.countDown();
            try {
                Assertions.assertTrue(releaseRemote.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            return ImmutableList.of(new Column("v.key", Type.INT));
        });
        Assertions.assertEquals(1, remoteStarted.getCount());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<ProcResult> result = executor.submit(node::fetchResult);
            Assertions.assertTrue(remoteStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertFalse(result.isDone());
            Assertions.assertEquals(1, base.getRows().size());
            releaseRemote.countDown();
            Assertions.assertEquals(2, result.get(5, TimeUnit.SECONDS).getRows().size());
        } finally {
            releaseRemote.countDown();
            executor.shutdownNow();
        }
    }
}
