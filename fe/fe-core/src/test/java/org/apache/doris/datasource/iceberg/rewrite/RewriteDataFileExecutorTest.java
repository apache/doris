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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class RewriteDataFileExecutorTest {

    @Test
    void testInvalidateTableCacheAfterCommit() throws Exception {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        IcebergTransaction transaction = Mockito.mock(IcebergTransaction.class);
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

            new RewriteDataFileExecutor(table, null).commitAndInvalidate(transaction);

            InOrder inOrder = Mockito.inOrder(transaction, cacheMgr);
            inOrder.verify(transaction).commit();
            inOrder.verify(cacheMgr).invalidateTableCache(table);
        }
    }
}
