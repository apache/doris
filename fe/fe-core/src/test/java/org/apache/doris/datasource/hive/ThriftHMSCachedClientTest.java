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

package org.apache.doris.datasource.hive;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;

public class ThriftHMSCachedClientTest {

    @Test
    public void testReturnsClientToPoolWhenPoolingEnabled() throws Exception {
        IMetaStoreClient client = Mockito.mock(IMetaStoreClient.class);
        Mockito.when(client.getAllDatabases()).thenReturn(Collections.emptyList());
        TestableThriftHMSCachedClient cachedClient = new TestableThriftHMSCachedClient(1, client);

        cachedClient.getAllDatabases();
        cachedClient.getAllDatabases();

        Assertions.assertEquals(1, cachedClient.getCreatedClientCount());
        Assertions.assertEquals(1, cachedClient.getClientPoolSize());
        Mockito.verify(client, Mockito.never()).close();

        cachedClient.close();

        Assertions.assertEquals(0, cachedClient.getClientPoolSize());
        Mockito.verify(client).close();
    }

    @Test
    public void testDoesNotReturnClientToPoolWhenPoolSizeIsZero() throws Exception {
        IMetaStoreClient firstClient = Mockito.mock(IMetaStoreClient.class);
        IMetaStoreClient secondClient = Mockito.mock(IMetaStoreClient.class);
        Mockito.when(firstClient.getAllDatabases()).thenReturn(Collections.emptyList());
        Mockito.when(secondClient.getAllDatabases()).thenReturn(Collections.emptyList());
        TestableThriftHMSCachedClient cachedClient = new TestableThriftHMSCachedClient(0, firstClient, secondClient);

        cachedClient.getAllDatabases();
        cachedClient.getAllDatabases();

        Assertions.assertEquals(2, cachedClient.getCreatedClientCount());
        Assertions.assertEquals(0, cachedClient.getClientPoolSize());
        Mockito.verify(firstClient).close();
        Mockito.verify(secondClient).close();
    }

    @Test
    public void testExceptionPathDoesNotReturnClientToPool() throws Exception {
        IMetaStoreClient client = Mockito.mock(IMetaStoreClient.class);
        Mockito.when(client.getAllDatabases()).thenThrow(new RuntimeException("boom"));
        TestableThriftHMSCachedClient cachedClient = new TestableThriftHMSCachedClient(1, client);

        Assertions.assertThrows(HMSClientException.class, cachedClient::getAllDatabases);

        Assertions.assertEquals(1, cachedClient.getCreatedClientCount());
        Assertions.assertEquals(0, cachedClient.getClientPoolSize());
        Mockito.verify(client).close();
    }

    private static class TestableThriftHMSCachedClient extends ThriftHMSCachedClient {
        private final Deque<IMetaStoreClient> clients = new ArrayDeque<>();
        private final AtomicInteger createdClientCount = new AtomicInteger();

        TestableThriftHMSCachedClient(int poolSize, IMetaStoreClient... clients) {
            super(new HiveConf(), poolSize, new ExecutionAuthenticator() {});
            this.clients.addAll(Arrays.asList(clients));
        }

        @Override
        protected IMetaStoreClient createClient(HiveConf hiveConf) {
            createdClientCount.incrementAndGet();
            IMetaStoreClient client = clients.pollFirst();
            if (client == null) {
                throw new AssertionError("No mock client available");
            }
            return client;
        }

        int getCreatedClientCount() {
            return createdClientCount.get();
        }
    }
}
