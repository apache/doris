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

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.property.metastore.HMSBaseProperties;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThriftHMSCachedClientTest {
    private MockMetastoreClientProvider provider;

    @Before
    public void setUp() {
        provider = new MockMetastoreClientProvider();
    }

    @Test
    public void testPoolConfigKeepsBorrowValidationAndIdleEvictionDisabled() {
        ThriftHMSCachedClient cachedClient = newClient(1);

        GenericObjectPool<?> pool = getPool(cachedClient);
        Assert.assertFalse(pool.getTestOnBorrow());
        Assert.assertFalse(pool.getTestOnReturn());
        Assert.assertFalse(pool.getTestWhileIdle());
        Assert.assertEquals(60_000L, pool.getMaxWaitMillis());
        Assert.assertEquals(-1L, pool.getTimeBetweenEvictionRunsMillis());
    }

    @Test
    public void testPoolDisabledCreatesAndClosesClientPerBorrow() throws Exception {
        ThriftHMSCachedClient cachedClient = newClient(0);

        Assert.assertNull(getPool(cachedClient));

        Object firstBorrowed = borrowClient(cachedClient);
        closeBorrowed(firstBorrowed);
        Assert.assertEquals(1, provider.createdClients.get());
        Assert.assertEquals(1, provider.closedClients.get());

        Object secondBorrowed = borrowClient(cachedClient);
        Assert.assertNotSame(firstBorrowed, secondBorrowed);
        closeBorrowed(secondBorrowed);
        Assert.assertEquals(2, provider.createdClients.get());
        Assert.assertEquals(2, provider.closedClients.get());
    }

    @Test
    public void testReturnObjectToPool() throws Exception {
        ThriftHMSCachedClient cachedClient = newClient(1);

        Object firstBorrowed = borrowClient(cachedClient);
        closeBorrowed(firstBorrowed);

        Assert.assertEquals(1, getPool(cachedClient).getNumIdle());
        Assert.assertEquals(0, getPool(cachedClient).getNumActive());
        Assert.assertEquals(1, provider.createdClients.get());
        Assert.assertEquals(0, provider.closedClients.get());

        Object secondBorrowed = borrowClient(cachedClient);
        Assert.assertSame(firstBorrowed, secondBorrowed);
        closeBorrowed(secondBorrowed);
    }

    @Test
    public void testInvalidateBrokenObject() throws Exception {
        ThriftHMSCachedClient cachedClient = newClient(1);

        Object brokenBorrowed = borrowClient(cachedClient);
        markBorrowedBroken(brokenBorrowed, new RuntimeException("broken"));
        closeBorrowed(brokenBorrowed);

        Assert.assertEquals(0, getPool(cachedClient).getNumIdle());
        Assert.assertEquals(0, getPool(cachedClient).getNumActive());
        Assert.assertEquals(1, provider.createdClients.get());
        Assert.assertEquals(1, provider.closedClients.get());

        Object nextBorrowed = borrowClient(cachedClient);
        Assert.assertNotSame(brokenBorrowed, nextBorrowed);
        Assert.assertEquals(2, provider.createdClients.get());
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
        Assert.assertEquals(1, provider.closedClients.get());
        Assert.assertThrows(IllegalStateException.class, () -> borrowClient(cachedClient));
    }

    @Test
    public void testCloseWhileObjectBorrowedClosesClientOnReturn() throws Exception {
        ThriftHMSCachedClient cachedClient = newClient(1);
        Object borrowed = borrowClient(cachedClient);

        cachedClient.close();
        Assert.assertEquals(0, provider.closedClients.get());

        closeBorrowed(borrowed);

        Assert.assertEquals(1, provider.closedClients.get());
        Assert.assertEquals(0, getPool(cachedClient).getNumIdle());
    }

    @Test
    public void testGetMetastoreClientClassName() {
        HiveConf hiveConf = new HiveConf();
        Assert.assertEquals(HiveMetaStoreClient.class.getName(),
                ThriftHMSCachedClient.getMetastoreClientClassName(hiveConf));

        hiveConf.set(HMSBaseProperties.HIVE_METASTORE_TYPE, HMSBaseProperties.GLUE_TYPE);
        Assert.assertEquals(AWSCatalogMetastoreClient.class.getName(),
                ThriftHMSCachedClient.getMetastoreClientClassName(hiveConf));

        hiveConf.set(HMSBaseProperties.HIVE_METASTORE_TYPE, HMSBaseProperties.DLF_TYPE);
        Assert.assertEquals(ProxyMetaStoreClient.class.getName(),
                ThriftHMSCachedClient.getMetastoreClientClassName(hiveConf));
    }

    @Test
    public void testUpdateTableStatisticsDoesNotBorrowSecondClient() {
        ThriftHMSCachedClient cachedClient = newClient(1);

        cachedClient.updateTableStatistics("db1", "tbl1", statistics -> statistics);

        Assert.assertEquals(1, provider.createdClients.get());
        Assert.assertEquals(1, getPool(cachedClient).getNumIdle());
        Assert.assertEquals(0, getPool(cachedClient).getNumActive());
    }

    @Test
    public void testAcquireSharedLockDoesNotBorrowSecondClient() {
        provider.lockStates.add(LockState.WAITING);
        provider.lockStates.add(LockState.ACQUIRED);
        ThriftHMSCachedClient cachedClient = newClient(1);

        cachedClient.acquireSharedLock("query-1", 1L, "user",
                new TableNameInfo("db1", "tbl1"), Collections.emptyList(), 5_000L);

        Assert.assertEquals(1, provider.createdClients.get());
        Assert.assertEquals(1, provider.checkLockCalls.get());
        Assert.assertEquals(1, getPool(cachedClient).getNumIdle());
        Assert.assertEquals(0, getPool(cachedClient).getNumActive());
    }

    @Test
    public void testUpdatePartitionStatisticsInvalidatesFailedClient() throws Exception {
        provider.alterPartitionFailure = new RuntimeException("alter partition failed");
        ThriftHMSCachedClient cachedClient = newClient(1);

        RuntimeException exception = Assert.assertThrows(RuntimeException.class,
                () -> cachedClient.updatePartitionStatistics("db1", "tbl1", "p1", statistics -> statistics));
        Assert.assertTrue(exception.getMessage().contains("failed to update table statistics"));
        assertBrokenBorrowerIsNotReused(cachedClient);
    }

    @Test
    public void testAddPartitionsInvalidatesFailedClient() throws Exception {
        provider.addPartitionsFailure = new RuntimeException("add partitions failed");
        ThriftHMSCachedClient cachedClient = newClient(1);

        RuntimeException exception = Assert.assertThrows(RuntimeException.class,
                () -> cachedClient.addPartitions("db1", "tbl1", Collections.singletonList(newPartitionWithStatistics())));
        Assert.assertTrue(exception.getMessage().contains("failed to add partitions"));
        assertBrokenBorrowerIsNotReused(cachedClient);
    }

    @Test
    public void testDropPartitionInvalidatesFailedClient() throws Exception {
        provider.dropPartitionFailure = new RuntimeException("drop partition failed");
        ThriftHMSCachedClient cachedClient = newClient(1);

        RuntimeException exception = Assert.assertThrows(RuntimeException.class,
                () -> cachedClient.dropPartition("db1", "tbl1", Collections.singletonList("p1"), false));
        Assert.assertTrue(exception.getMessage().contains("failed to drop partition"));
        assertBrokenBorrowerIsNotReused(cachedClient);
    }

    private void assertBrokenBorrowerIsNotReused(ThriftHMSCachedClient cachedClient) throws Exception {
        Assert.assertEquals(0, getPool(cachedClient).getNumIdle());
        Assert.assertEquals(0, getPool(cachedClient).getNumActive());
        Assert.assertEquals(1, provider.createdClients.get());
        Assert.assertEquals(1, provider.closedClients.get());

        Object nextBorrowed = borrowClient(cachedClient);
        Assert.assertEquals(2, provider.createdClients.get());
        closeBorrowed(nextBorrowed);
    }

    private ThriftHMSCachedClient newClient(int poolSize) {
        return newClient(new HiveConf(), poolSize);
    }

    private ThriftHMSCachedClient newClient(HiveConf hiveConf, int poolSize) {
        return new ThriftHMSCachedClient(hiveConf, poolSize, new ExecutionAuthenticator() {
        }, provider);
    }

    private GenericObjectPool<?> getPool(ThriftHMSCachedClient cachedClient) {
        return Deencapsulation.getField(cachedClient, "clientPool");
    }

    private Object borrowClient(ThriftHMSCachedClient cachedClient) {
        return Deencapsulation.invoke(cachedClient, "getClient");
    }

    private void markBorrowedBroken(Object borrowedClient, Throwable throwable) {
        Deencapsulation.invoke(borrowedClient, "setThrowable", throwable);
    }

    private void closeBorrowed(Object borrowedClient) throws Exception {
        ((AutoCloseable) borrowedClient).close();
    }

    private HivePartitionWithStatistics newPartitionWithStatistics() {
        HivePartition partition = new HivePartition(
                NameMapping.createForTest("db1", "tbl1"),
                false,
                "input-format",
                "file:///tmp/part",
                Collections.singletonList("p1"),
                new HashMap<>(),
                "output-format",
                "serde",
                Collections.singletonList(new FieldSchema("c1", "string", "")));
        return new HivePartitionWithStatistics("k1=v1", partition, HivePartitionStatistics.EMPTY);
    }

    private static class MockMetastoreClientProvider implements ThriftHMSCachedClient.MetaStoreClientProvider {
        private final AtomicInteger createdClients = new AtomicInteger();
        private final AtomicInteger closedClients = new AtomicInteger();
        private final AtomicInteger checkLockCalls = new AtomicInteger();
        private final Deque<LockState> lockStates = new ArrayDeque<>();

        private volatile RuntimeException alterPartitionFailure;
        private volatile RuntimeException addPartitionsFailure;
        private volatile RuntimeException dropPartitionFailure;

        @Override
        public IMetaStoreClient create(HiveConf hiveConf) {
            createdClients.incrementAndGet();
            return (IMetaStoreClient) Proxy.newProxyInstance(
                    IMetaStoreClient.class.getClassLoader(),
                    new Class[] {IMetaStoreClient.class},
                    (proxy, method, args) -> handleMethod(proxy, method.getName(), args, method.getReturnType()));
        }

        private Object handleMethod(Object proxy, String methodName, Object[] args, Class<?> returnType) {
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
            if ("getPartitionsByNames".equals(methodName)) {
                Partition partition = new Partition();
                partition.setParameters(new HashMap<>());
                return Collections.singletonList(partition);
            }
            if ("alter_partition".equals(methodName)) {
                if (alterPartitionFailure != null) {
                    throw alterPartitionFailure;
                }
                return null;
            }
            if ("add_partitions".equals(methodName)) {
                if (addPartitionsFailure != null) {
                    throw addPartitionsFailure;
                }
                return 1;
            }
            if ("dropPartition".equals(methodName)) {
                if (dropPartitionFailure != null) {
                    throw dropPartitionFailure;
                }
                return true;
            }
            if ("lock".equals(methodName)) {
                return newLockResponse(nextLockState());
            }
            if ("checkLock".equals(methodName)) {
                checkLockCalls.incrementAndGet();
                return newLockResponse(nextLockState());
            }
            return defaultValue(returnType);
        }

        private LockState nextLockState() {
            synchronized (lockStates) {
                if (lockStates.isEmpty()) {
                    return LockState.ACQUIRED;
                }
                return lockStates.removeFirst();
            }
        }

        private LockResponse newLockResponse(LockState state) {
            LockResponse response = new LockResponse();
            response.setLockid(1L);
            response.setState(state);
            return response;
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
}
