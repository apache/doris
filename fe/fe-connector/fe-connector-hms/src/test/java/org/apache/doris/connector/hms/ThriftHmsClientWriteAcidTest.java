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

package org.apache.doris.connector.hms;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests the write / read-ACID primitives added to {@link ThriftHmsClient} — the SPI-clean port of
 * fe-core {@code ThriftHMSCachedClient}'s add-partition / statistics / txn / lock / valid-write-id
 * calls.
 *
 * <p>WHY: these primitives are the metastore mutations behind a Hive INSERT commit and a
 * transactional-read snapshot. A dropped argument, a wrong forward, or a lost graceful-degradation
 * fallback silently corrupts a commit or fails a scan. The tests inject a recording fake
 * {@link IMetaStoreClient} (a JDK {@link Proxy}; no Mockito in the connector modules) via the
 * package-private client-provider seam with pooling disabled, then assert exactly what each primitive
 * forwards to the metastore and what it returns — the behavior contract the connector transaction
 * depends on (Rule 9).</p>
 */
public class ThriftHmsClientWriteAcidTest {

    // ---- openTxn / commitTxn --------------------------------------------------------------------

    @Test
    public void testOpenTxnForwardsUserAndReturnsId() {
        RecordingClient fake = new RecordingClient().stub("openTxn", 42L);
        ThriftHmsClient client = newClient(fake);

        long txnId = client.openTxn("alice");

        Assertions.assertEquals(42L, txnId);
        Assertions.assertEquals("alice", argsOf(fake, "openTxn")[0]);
    }

    @Test
    public void testCommitTxnForwardsId() {
        RecordingClient fake = new RecordingClient();
        ThriftHmsClient client = newClient(fake);

        client.commitTxn(7L);

        Assertions.assertEquals(7L, argsOf(fake, "commitTxn")[0]);
    }

    // ---- dropPartition / partitionExists --------------------------------------------------------

    @Test
    public void testDropPartitionForwardsArgsAndReturnsResult() {
        RecordingClient fake = new RecordingClient().stub("dropPartition", Boolean.TRUE);
        ThriftHmsClient client = newClient(fake);

        boolean dropped = client.dropPartition("db", "t",
                Collections.singletonList("2024"), false);

        Assertions.assertTrue(dropped);
        Object[] args = argsOf(fake, "dropPartition");
        Assertions.assertEquals("db", args[0]);
        Assertions.assertEquals("t", args[1]);
        Assertions.assertEquals(Collections.singletonList("2024"), args[2]);
        Assertions.assertEquals(false, args[3]);
    }

    @Test
    public void testPartitionExistsTrueWhenFound() {
        RecordingClient fake = new RecordingClient().stub("getPartition", new Partition());
        ThriftHmsClient client = newClient(fake);

        Assertions.assertTrue(client.partitionExists("db", "t",
                Collections.singletonList("2024")));
    }

    @Test
    public void testPartitionExistsFalseOnNoSuchObject() {
        // A not-found probe must swallow NoSuchObjectException and return false (drives the
        // NEW->APPEND downgrade), not propagate it as a client failure.
        RecordingClient fake = new RecordingClient()
                .stub("getPartition", new NoSuchObjectException("no such partition"));
        ThriftHmsClient client = newClient(fake);

        Assertions.assertFalse(client.partitionExists("db", "t",
                Collections.singletonList("2024")));
    }

    // ---- addPartitions --------------------------------------------------------------------------

    @Test
    public void testAddPartitionsBuildsMetastorePartitions() {
        RecordingClient fake = new RecordingClient().stub("add_partitions", 1);
        ThriftHmsClient client = newClient(fake);

        HmsPartitionWithStatistics part = HmsPartitionWithStatistics.builder()
                .partitionValues(Collections.singletonList("2024"))
                .location("hdfs://ns/db/t/dt=2024")
                .columns(Collections.emptyList())
                .inputFormat("in")
                .outputFormat("out")
                .serde("serde")
                .parameters(new HashMap<>())
                .statistics(HmsPartitionStatistics.fromCommonStatistics(10, 2, 100))
                .build();

        client.addPartitions("db", "t", Collections.singletonList(part));

        Object[] args = argsOf(fake, "add_partitions");
        @SuppressWarnings("unchecked")
        List<Partition> sent = (List<Partition>) args[0];
        Assertions.assertEquals(1, sent.size());
        Partition p = sent.get(0);
        Assertions.assertEquals("db", p.getDbName());
        Assertions.assertEquals("t", p.getTableName());
        Assertions.assertEquals(Collections.singletonList("2024"), p.getValues());
        Assertions.assertEquals("hdfs://ns/db/t/dt=2024", p.getSd().getLocation());
        Assertions.assertEquals("10", p.getParameters().get(StatsSetupConst.ROW_COUNT));
    }

    @Test
    public void testAddPartitionsBatchesInChunksOfTwenty() {
        // 45 partitions -> ceil(45/20) = 3 add_partitions calls; a single giant call can exceed the
        // metastore's thrift message limit.
        RecordingClient fake = new RecordingClient().stub("add_partitions", 0);
        ThriftHmsClient client = newClient(fake);

        List<HmsPartitionWithStatistics> many = new ArrayList<>();
        for (int i = 0; i < 45; i++) {
            many.add(HmsPartitionWithStatistics.builder()
                    .partitionValues(Collections.singletonList("p" + i))
                    .columns(Collections.emptyList())
                    .parameters(new HashMap<>())
                    .statistics(HmsPartitionStatistics.EMPTY)
                    .build());
        }

        client.addPartitions("db", "t", many);

        // 3 chunks (20 + 20 + 5), and — the load-bearing contract — no partition is lost or
        // duplicated across the chunk boundaries: the union of forwarded partitions is exactly p0..p44.
        List<String> forwarded = new ArrayList<>();
        long calls = 0;
        for (int idx = 0; idx < fake.methodNames.size(); idx++) {
            if (!"add_partitions".equals(fake.methodNames.get(idx))) {
                continue;
            }
            calls++;
            @SuppressWarnings("unchecked")
            List<Partition> batch = (List<Partition>) fake.argsList.get(idx)[0];
            Assertions.assertTrue(batch.size() <= 20, "a batch exceeded ADD_PARTITIONS_BATCH_SIZE");
            for (Partition p : batch) {
                forwarded.add(p.getValues().get(0));
            }
        }
        Assertions.assertEquals(3, calls);
        Assertions.assertEquals(45, forwarded.size());
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 45; i++) {
            expected.add("p" + i);
        }
        Assertions.assertEquals(expected, new HashSet<>(forwarded));
    }

    // ---- table / partition statistics -----------------------------------------------------------

    @Test
    public void testUpdateTableStatisticsRebuildsParamsAndAlters() {
        RecordingClient fake = new RecordingClient();
        Table origin = new Table();
        origin.setParameters(new HashMap<>());
        fake.stub("getTable", origin);
        ThriftHmsClient client = newClient(fake);

        client.updateTableStatistics("db", "t",
                current -> HmsPartitionStatistics.fromCommonStatistics(5, 1, 50));

        Object[] args = argsOf(fake, "alter_table");
        Assertions.assertEquals("db", args[0]);
        Assertions.assertEquals("t", args[1]);
        Table altered = (Table) args[2];
        Assertions.assertEquals("5", altered.getParameters().get(StatsSetupConst.ROW_COUNT));
        Assertions.assertEquals("1", altered.getParameters().get(StatsSetupConst.NUM_FILES));
        Assertions.assertEquals("50", altered.getParameters().get(StatsSetupConst.TOTAL_SIZE));
        Assertions.assertTrue(altered.getParameters().containsKey("transient_lastDdlTime"));
    }

    @Test
    public void testUpdatePartitionStatisticsRebuildsParamsAndAlters() {
        RecordingClient fake = new RecordingClient();
        Partition origin = new Partition();
        origin.setParameters(new HashMap<>());
        fake.stub("getPartitionsByNames", Collections.singletonList(origin));
        ThriftHmsClient client = newClient(fake);

        client.updatePartitionStatistics("db", "t", "dt=2024",
                current -> HmsPartitionStatistics.fromCommonStatistics(3, 1, 30));

        Object[] args = argsOf(fake, "alter_partition");
        Partition altered = (Partition) args[2];
        Assertions.assertEquals("3", altered.getParameters().get(StatsSetupConst.ROW_COUNT));
        Assertions.assertEquals("30", altered.getParameters().get(StatsSetupConst.TOTAL_SIZE));
    }

    @Test
    public void testUpdatePartitionStatisticsRejectsAmbiguousName() {
        RecordingClient fake = new RecordingClient();
        fake.stub("getPartitionsByNames", new ArrayList<Partition>()); // size 0 != 1
        ThriftHmsClient client = newClient(fake);

        Assertions.assertThrows(HmsClientException.class, () ->
                client.updatePartitionStatistics("db", "t", "dt=2024",
                        current -> HmsPartitionStatistics.EMPTY));
    }

    // ---- getValidWriteIds -----------------------------------------------------------------------

    @Test
    public void testGetValidWriteIdsSuccessPathEmitsSnapshotThenWriteIds() {
        // Primary read-ACID path: a compatible metastore returns exactly one write-id list. A
        // distinctive snapshot (high-watermark 100, NOT the fallback's Long.MAX_VALUE) makes this test
        // fail if the code silently degrades to the fallback branch.
        ValidReadTxnList snapshot = new ValidReadTxnList(new long[0], new BitSet(), 100L, 5L);
        TableValidWriteIds writeIds = new TableValidWriteIds(
                "db.t", 100L, Collections.emptyList(), ByteBuffer.allocate(0));
        RecordingClient fake = new RecordingClient()
                .stub("getValidTxns", snapshot)
                .stub("getValidWriteIds", Collections.singletonList(writeIds));
        ThriftHmsClient client = newClient(fake);

        Map<String, String> conf = client.getValidWriteIds("db.t", 42L);

        // (a) The recent snapshot string — NOT currentTransactionId (42) — drives the write-id lookup.
        Object[] gvwiArgs = argsOf(fake, "getValidWriteIds");
        Assertions.assertEquals(Collections.singletonList("db.t"), gvwiArgs[0]);
        Assertions.assertEquals(snapshot.toString(), gvwiArgs[1]);
        // (b) VALID_TXNS_KEY carries the txn snapshot; VALID_WRITEIDS_KEY carries the write-id list —
        // no key swap. Both are the SUCCESS values (snapshot watermark 100), not the fallback watermark.
        Assertions.assertEquals(snapshot.writeToString(), conf.get(HmsAcidConstants.VALID_TXNS_KEY));
        Assertions.assertTrue(conf.get(HmsAcidConstants.VALID_WRITEIDS_KEY).contains("db.t"));
        Assertions.assertNotEquals(conf.get(HmsAcidConstants.VALID_TXNS_KEY),
                conf.get(HmsAcidConstants.VALID_WRITEIDS_KEY));
    }

    @Test
    public void testGetValidWriteIdsFallsBackToMaxWatermark() {
        // An incompatible metastore returns an unexpected write-id list; rather than fail the scan,
        // getValidWriteIds must degrade to a max watermark and still emit both config keys.
        ValidReadTxnList snapshot = new ValidReadTxnList();
        RecordingClient fake = new RecordingClient()
                .stub("getValidTxns", snapshot)
                .stub("getValidWriteIds", new ArrayList<>()); // size 0 -> triggers fallback
        ThriftHmsClient client = newClient(fake);

        Map<String, String> conf = client.getValidWriteIds("db.t", 5L);

        // The recent snapshot (not currentTransactionId) was still forwarded before the size guard threw.
        Object[] gvwiArgs = argsOf(fake, "getValidWriteIds");
        Assertions.assertEquals(Collections.singletonList("db.t"), gvwiArgs[0]);
        Assertions.assertEquals(snapshot.toString(), gvwiArgs[1]);
        // Both BE-contract keys are present, and the fallback write-id list is scoped to the table.
        Assertions.assertNotNull(conf.get(HmsAcidConstants.VALID_TXNS_KEY));
        Assertions.assertNotNull(conf.get(HmsAcidConstants.VALID_WRITEIDS_KEY));
        Assertions.assertTrue(conf.get(HmsAcidConstants.VALID_WRITEIDS_KEY).contains("db.t"));
    }

    // ---- acquireSharedLock ----------------------------------------------------------------------

    @Test
    public void testAcquireSharedLockBuildsSharedReadComponentsAndReturnsWhenAcquired() {
        RecordingClient fake = new RecordingClient()
                .stub("lock", new LockResponse(1L, LockState.ACQUIRED));
        ThriftHmsClient client = newClient(fake);

        client.acquireSharedLock("q1", 5L, "alice", "db", "t",
                Collections.emptyList(), 1000L);

        LockRequest request = (LockRequest) argsOf(fake, "lock")[0];
        Assertions.assertEquals(1, request.getComponent().size());
        LockComponent component = request.getComponent().get(0);
        Assertions.assertEquals("db", component.getDbname());
        Assertions.assertEquals("t", component.getTablename());
        Assertions.assertEquals(LockType.SHARED_READ, component.getType());
        Assertions.assertEquals(DataOperationType.SELECT, component.getOperationType());
    }

    @Test
    public void testAcquireSharedLockOneComponentPerPartition() {
        RecordingClient fake = new RecordingClient()
                .stub("lock", new LockResponse(1L, LockState.ACQUIRED));
        ThriftHmsClient client = newClient(fake);

        client.acquireSharedLock("q1", 5L, "alice", "db", "t",
                java.util.Arrays.asList("dt=1", "dt=2"), 1000L);

        LockRequest request = (LockRequest) argsOf(fake, "lock")[0];
        Assertions.assertEquals(2, request.getComponent().size());
        Assertions.assertEquals("dt=1", request.getComponent().get(0).getPartitionname());
        Assertions.assertEquals("dt=2", request.getComponent().get(1).getPartitionname());
    }

    @Test
    public void testAcquireSharedLockPollsUntilAcquired() {
        RecordingClient fake = new RecordingClient()
                .stub("lock", new LockResponse(2L, LockState.WAITING))
                .stub("checkLock", new LockResponse(2L, LockState.ACQUIRED));
        ThriftHmsClient client = newClient(fake);

        client.acquireSharedLock("q", 5L, "u", "db", "t", Collections.emptyList(), 5000L);

        Assertions.assertTrue(fake.methodNames.contains("checkLock"));
    }

    @Test
    public void testAcquireSharedLockTimesOut() {
        RecordingClient fake = new RecordingClient()
                .stub("lock", new LockResponse(3L, LockState.WAITING))
                .stub("checkLock", new LockResponse(3L, LockState.WAITING));
        ThriftHmsClient client = newClient(fake);

        // timeoutMs = -1 => the elapsed guard trips on the first poll iteration, deterministically.
        Assertions.assertThrows(HmsClientException.class, () ->
                client.acquireSharedLock("q", 5L, "u", "db", "t", Collections.emptyList(), -1L));
    }

    // ---- harness --------------------------------------------------------------------------------

    private static ThriftHmsClient newClient(RecordingClient handler) {
        IMetaStoreClient fake = (IMetaStoreClient) Proxy.newProxyInstance(
                IMetaStoreClient.class.getClassLoader(),
                new Class<?>[] {IMetaStoreClient.class},
                handler);
        // poolSize 0 -> no pool: every call creates a fresh client via the provider (our fake).
        HmsClientConfig config = new HmsClientConfig(new HashMap<>(), 0);
        return new ThriftHmsClient(config, null, hiveConf -> fake, HmsTypeMapping.Options.DEFAULT);
    }

    private static Object[] argsOf(RecordingClient handler, String method) {
        int idx = handler.methodNames.indexOf(method);
        Assertions.assertTrue(idx >= 0, "expected a call to " + method);
        return handler.argsList.get(idx);
    }

    /**
     * A recording fake {@link IMetaStoreClient}: records every method name + args in call order and
     * returns per-method canned values (a {@link Throwable} value is thrown from the call).
     * Implemented as an {@link InvocationHandler} because {@code IMetaStoreClient} has hundreds of
     * methods — an anonymous class is impractical and Mockito is not on the connector test path.
     */
    private static final class RecordingClient implements InvocationHandler {
        private final List<String> methodNames = new ArrayList<>();
        private final List<Object[]> argsList = new ArrayList<>();
        private final Map<String, Object> responses = new HashMap<>();

        RecordingClient stub(String method, Object value) {
            responses.put(method, value);
            return this;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if (method.getDeclaringClass() == Object.class) {
                switch (name) {
                    case "toString":
                        return "RecordingClient";
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "equals":
                        return proxy == args[0];
                    default:
                        return null;
                }
            }
            if ("close".equals(name)) {
                return null;
            }
            methodNames.add(name);
            argsList.add(args == null ? new Object[0] : args);
            if (responses.containsKey(name)) {
                Object value = responses.get(name);
                if (value instanceof Throwable) {
                    throw (Throwable) value;
                }
                return value;
            }
            return defaultValue(method.getReturnType());
        }

        private static Object defaultValue(Class<?> type) {
            if (!type.isPrimitive() || type == void.class) {
                return null;
            }
            if (type == boolean.class) {
                return false;
            }
            if (type == long.class) {
                return 0L;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == short.class) {
                return (short) 0;
            }
            if (type == byte.class) {
                return (byte) 0;
            }
            if (type == char.class) {
                return (char) 0;
            }
            if (type == float.class) {
                return 0f;
            }
            return 0d;
        }
    }
}
