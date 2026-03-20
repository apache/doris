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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;

import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThriftHMSCachedClientTest {
    private AtomicInteger createdClients;
    private AtomicInteger closedClients;

    @Before
    public void setUp() {
        createdClients = new AtomicInteger();
        closedClients = new AtomicInteger();
        new MockUp<RetryingMetaStoreClient>() {
            @Mock
            public IMetaStoreClient getProxy(HiveConf hiveConf, HiveMetaHookLoader hookLoader, String mscClassName) {
                return createMockClient();
            }
        };
    }

    @Test
    public void testReturnObjectToPool() throws Exception {
        ThriftHMSCachedClient cachedClient = newClient(1);

        Object firstBorrowed = borrowClient(cachedClient);
        closeBorrowed(firstBorrowed);

        Assert.assertEquals(1, getPool(cachedClient).getNumIdle());
        Assert.assertEquals(0, getPool(cachedClient).getNumActive());
        Assert.assertEquals(1, createdClients.get());
        Assert.assertEquals(0, closedClients.get());

        Object secondBorrowed = borrowClient(cachedClient);
        Assert.assertSame(firstBorrowed, secondBorrowed);
        closeBorrowed(secondBorrowed);
    }

    @Test
    public void testInvalidateBrokenObject() throws Exception {
        ThriftHMSCachedClient cachedClient = newClient(1);

        Object brokenBorrowed = borrowClient(cachedClient);
        markBorrowedBroken(brokenBorrowed);
        closeBorrowed(brokenBorrowed);

        Assert.assertEquals(0, getPool(cachedClient).getNumIdle());
        Assert.assertEquals(0, getPool(cachedClient).getNumActive());
        Assert.assertEquals(1, createdClients.get());
        Assert.assertEquals(1, closedClients.get());

        Object nextBorrowed = borrowClient(cachedClient);
        Assert.assertNotSame(brokenBorrowed, nextBorrowed);
        Assert.assertEquals(2, createdClients.get());
        closeBorrowed(nextBorrowed);
    }

    @Test
    public void testBorrowBlocksUntilObjectReturned() throws Exception {
        ThriftHMSCachedClient cachedClient = newClient(1);
        Object firstBorrowed = borrowClient(cachedClient);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Object> waitingBorrow = executor.submit(() -> borrowClient(cachedClient));
            Thread.sleep(200L);
            Assert.assertFalse(waitingBorrow.isDone());

            closeBorrowed(firstBorrowed);

            Object secondBorrowed = waitingBorrow.get(2, TimeUnit.SECONDS);
            Assert.assertSame(firstBorrowed, secondBorrowed);
            closeBorrowed(secondBorrowed);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCloseDestroysIdleObjectsAndRejectsBorrow() throws Exception {
        ThriftHMSCachedClient cachedClient = newClient(1);
        Object borrowed = borrowClient(cachedClient);
        closeBorrowed(borrowed);

        cachedClient.close();

        Assert.assertTrue(getPool(cachedClient).isClosed());
        Assert.assertEquals(1, closedClients.get());
        Assert.assertThrows(IllegalStateException.class, () -> borrowClient(cachedClient));
    }

    @Test
    public void testCloseWhileObjectBorrowedClosesClientOnReturn() throws Exception {
        ThriftHMSCachedClient cachedClient = newClient(1);
        Object borrowed = borrowClient(cachedClient);

        cachedClient.close();
        Assert.assertEquals(0, closedClients.get());

        closeBorrowed(borrowed);

        Assert.assertEquals(1, closedClients.get());
        Assert.assertEquals(0, getPool(cachedClient).getNumIdle());
    }

    @Test
    public void testUpdateTableStatisticsDoesNotBorrowSecondClient() {
        ThriftHMSCachedClient cachedClient = newClient(1);

        cachedClient.updateTableStatistics("db1", "tbl1", statistics -> statistics);

        Assert.assertEquals(1, createdClients.get());
        Assert.assertEquals(1, getPool(cachedClient).getNumIdle());
        Assert.assertEquals(0, getPool(cachedClient).getNumActive());
    }

    private ThriftHMSCachedClient newClient(int poolSize) {
        return new ThriftHMSCachedClient(new HiveConf(), poolSize, new ExecutionAuthenticator() {
        });
    }

    private GenericObjectPool<?> getPool(ThriftHMSCachedClient cachedClient) {
        return Deencapsulation.getField(cachedClient, "clientPool");
    }

    private Object borrowClient(ThriftHMSCachedClient cachedClient) {
        return Deencapsulation.invoke(cachedClient, "getClient");
    }

    private void markBorrowedBroken(Object borrowedClient) {
        Deencapsulation.invoke(borrowedClient, "setThrowable", new RuntimeException("broken"));
    }

    private void closeBorrowed(Object borrowedClient) throws Exception {
        ((AutoCloseable) borrowedClient).close();
    }

    private IMetaStoreClient createMockClient() {
        createdClients.incrementAndGet();
        return (IMetaStoreClient) Proxy.newProxyInstance(
                IMetaStoreClient.class.getClassLoader(),
                new Class[] {IMetaStoreClient.class},
                (proxy, method, args) -> {
                    String methodName = method.getName();
                    if ("close".equals(methodName)) {
                        closedClients.incrementAndGet();
                        return null;
                    }
                    if ("hashCode".equals(methodName)) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(methodName)) {
                        return proxy == args[0];
                    }
                    if ("toString".equals(methodName)) {
                        return "MockHmsClient";
                    }
                    if ("getTable".equals(methodName)) {
                        Table table = new Table();
                        table.setParameters(new HashMap<>());
                        return table;
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    private Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0F;
        }
        if (returnType == double.class) {
            return 0D;
        }
        if (returnType == char.class) {
            return '\0';
        }
        return null;
    }
}
