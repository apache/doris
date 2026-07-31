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

import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MaterializedIndexTest {

    private MaterializedIndex index;
    private long indexId;

    private List<Column> columns;
    private Env env = Mockito.mock(Env.class);

    private FakeEnv fakeEnv;

    @Before
    public void setUp() {
        indexId = 10000;

        columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""));
        index = new MaterializedIndex(indexId, IndexState.NORMAL);

        fakeEnv = new FakeEnv();
        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @After
    public void tearDown() {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
    }

    @Test
    public void getMethodTest() {
        Assert.assertEquals(indexId, index.getId());
    }

    @Test
    public void testGetTabletsReturnsImmutableSnapshot() {
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, 1, TStorageMedium.HDD);
        index.addTablet(new LocalTablet(1L), tabletMeta, true);

        List<Tablet> snapshot = index.getTablets();
        Assert.assertEquals(1, snapshot.size());

        // A write after the snapshot was taken must not be visible in it (copy-on-write).
        index.addTablet(new LocalTablet(2L), tabletMeta, true);
        Assert.assertEquals(1, snapshot.size());
        Assert.assertEquals(2, index.getTablets().size());

        // The returned snapshot is read-only.
        Assert.assertThrows(UnsupportedOperationException.class, () -> snapshot.add(new LocalTablet(3L)));
    }

    @Test
    public void testPartitionMetaChecksum() {
        MaterializedIndex firstIndex = new MaterializedIndex(1L, IndexState.NORMAL);
        LocalTablet firstTablet = new LocalTablet(10L);
        firstTablet.addReplica(new LocalReplica(100L, 1000L, Replica.ReplicaState.NORMAL, 1L, 1), true);
        firstTablet.addReplica(new LocalReplica(101L, 1001L, Replica.ReplicaState.NORMAL, 1L, 1), true);
        firstIndex.addTablet(firstTablet, null, true);
        LocalTablet secondTablet = new LocalTablet(11L);
        secondTablet.addReplica(new LocalReplica(110L, 1010L, Replica.ReplicaState.NORMAL, 1L, 1), true);
        firstIndex.addTablet(secondTablet, null, true);
        Partition firstPartition = new Partition(1L, "p1", firstIndex, null);
        firstPartition.createRollupIndex(createIndex(2L, 20L, 200L, 2000L));
        firstPartition.createRollupIndex(createIndex(3L, 30L, 300L, 3000L));
        // Pin visibleVersionTime so partitions constructed in different millis stay comparable.
        long pinnedVisibleVersionTime = firstPartition.getVisibleVersionTime();
        Partition deserializedPartition = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(firstPartition),
                Partition.class);
        Assert.assertEquals(firstPartition.getMetaChecksum(), deserializedPartition.getMetaChecksum());

        MaterializedIndex reorderedIndex = new MaterializedIndex(1L, IndexState.NORMAL);
        LocalTablet reorderedSecondTablet = new LocalTablet(11L);
        reorderedSecondTablet.addReplica(new LocalReplica(110L, 1010L, Replica.ReplicaState.NORMAL, 1L, 1), true);
        reorderedIndex.addTablet(reorderedSecondTablet, null, true);
        LocalTablet reorderedTablet = new LocalTablet(10L);
        reorderedTablet.addReplica(new LocalReplica(101L, 1001L, Replica.ReplicaState.NORMAL, 1L, 1), true);
        reorderedTablet.addReplica(new LocalReplica(100L, 1000L, Replica.ReplicaState.NORMAL, 1L, 1), true);
        reorderedIndex.addTablet(reorderedTablet, null, true);
        Partition reorderedPartition = new Partition(1L, "p1", reorderedIndex, null);
        reorderedPartition.setVisibleVersionAndTime(reorderedPartition.getVisibleVersion(), pinnedVisibleVersionTime);
        reorderedPartition.createRollupIndex(createIndex(3L, 30L, 300L, 3000L));
        reorderedPartition.createRollupIndex(createIndex(2L, 20L, 200L, 2000L));
        Assert.assertEquals(firstPartition.getMetaChecksum(), reorderedPartition.getMetaChecksum());

        MaterializedIndex movedIndex = new MaterializedIndex(1L, IndexState.NORMAL);
        LocalTablet movedTablet = new LocalTablet(10L);
        movedTablet.addReplica(new LocalReplica(100L, 1000L, Replica.ReplicaState.NORMAL, 1L, 1), true);
        movedTablet.addReplica(new LocalReplica(102L, 1002L, Replica.ReplicaState.NORMAL, 1L, 1), true);
        movedIndex.addTablet(movedTablet, null, true);
        movedIndex.addTablet(secondTablet, null, true);
        Partition movedPartition = new Partition(1L, "p1", movedIndex, null);
        movedPartition.setVisibleVersionAndTime(movedPartition.getVisibleVersion(), pinnedVisibleVersionTime);
        movedPartition.createRollupIndex(createIndex(2L, 20L, 200L, 2000L));
        movedPartition.createRollupIndex(createIndex(3L, 30L, 300L, 3000L));
        Assert.assertNotEquals(firstPartition.getMetaChecksum(), movedPartition.getMetaChecksum());

        firstPartition.setRemoteMetaChecksum(firstPartition.getMetaChecksum());
        Assert.assertEquals(firstPartition.getMetaChecksum(), firstPartition.getRemoteMetaChecksum());
    }

    @Test
    public void testPartitionMetaChecksumChangesOnReplicaQueryFields() {
        // Build a partition with one tablet/replica.
        MaterializedIndex baseIndex = new MaterializedIndex(1L, IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(10L);
        LocalReplica replica = new LocalReplica(100L, 1000L, Replica.ReplicaState.NORMAL, 1L, 1);
        tablet.addReplica(replica, true);
        baseIndex.addTablet(tablet, null, true);
        Partition partition = new Partition(1L, "p1", baseIndex, null);
        String original = partition.getMetaChecksum();

        // 1) lastFailedVersion change must invalidate the checksum.
        replica.updateLastFailedVersion(5L);
        Assert.assertNotEquals(original, partition.getMetaChecksum());
        replica.updateLastFailedVersion(-1L);
        Assert.assertEquals(original, partition.getMetaChecksum());

        // 2) state change must invalidate the checksum.
        replica.setState(Replica.ReplicaState.DECOMMISSION);
        Assert.assertNotEquals(original, partition.getMetaChecksum());
        replica.setState(Replica.ReplicaState.NORMAL);
        Assert.assertEquals(original, partition.getMetaChecksum());

        // 3) bad flag change must invalidate the checksum.
        Assert.assertTrue(replica.setBad(true));
        Assert.assertNotEquals(original, partition.getMetaChecksum());
        Assert.assertTrue(replica.setBad(false));
        Assert.assertEquals(original, partition.getMetaChecksum());

        // 4) pathHash change must invalidate the checksum.
        replica.setPathHash(99L);
        Assert.assertNotEquals(original, partition.getMetaChecksum());
        replica.setPathHash(-1L);
        Assert.assertEquals(original, partition.getMetaChecksum());

        // 5) version change must invalidate the checksum. Replica.updateVersion()
        // refuses to roll back, so this is asserted last with a one-way change.
        replica.updateVersion(7L);
        Assert.assertNotEquals(original, partition.getMetaChecksum());
    }

    @Test
    public void testPartitionMetaChecksumChangesOnPartitionTopLevelFields() {
        MaterializedIndex baseIndex = new MaterializedIndex(1L, IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(10L);
        tablet.addReplica(new LocalReplica(100L, 1000L, Replica.ReplicaState.NORMAL, 1L, 1), true);
        baseIndex.addTablet(tablet, null, true);
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(3);
        Partition partition = new Partition(1L, "p1", baseIndex, distributionInfo);
        String original = partition.getMetaChecksum();

        // RENAME PARTITION only mutates the partition name, with no visible version change;
        // the checksum must still change so the remote cache can detect the rename.
        partition.setName("p1_renamed");
        Assert.assertNotEquals(original, partition.getMetaChecksum());
        partition.setName("p1");
        Assert.assertEquals(original, partition.getMetaChecksum());

        // PartitionState changes (e.g. RESTORE) must invalidate the checksum.
        partition.setState(Partition.PartitionState.RESTORE);
        Assert.assertNotEquals(original, partition.getMetaChecksum());
        partition.setState(Partition.PartitionState.NORMAL);
        Assert.assertEquals(original, partition.getMetaChecksum());

        // DistributionInfo bucket-num change must invalidate the checksum.
        int oldBucketNum = distributionInfo.getBucketNum();
        distributionInfo.setBucketNum(oldBucketNum + 2);
        Assert.assertNotEquals(original, partition.getMetaChecksum());
        distributionInfo.setBucketNum(oldBucketNum);
        Assert.assertEquals(original, partition.getMetaChecksum());

        // nextVersion changes must invalidate the checksum (asserted last;
        // setNextVersion() can't be reverted to its original value safely).
        partition.setNextVersion(partition.getNextVersion() + 1);
        Assert.assertNotEquals(original, partition.getMetaChecksum());
    }

    private MaterializedIndex createIndex(long indexId, long tabletId, long replicaId, long backendId) {
        MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(new LocalReplica(replicaId, backendId, Replica.ReplicaState.NORMAL, 1L, 1), true);
        index.addTablet(tablet, null, true);
        return index;
    }

    @Test
    public void testConcurrentGetTabletsNeverThrows() throws InterruptedException {
        // A reader repeatedly snapshots and iterates getTablets() while a writer keeps
        // adding tablets. Copy-on-write guarantees the reader never observes a partially
        // built list or throws ConcurrentModificationException.
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, 1, TStorageMedium.HDD);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean(false);

        Thread writer = new Thread(() -> {
            long id = 1000L;
            while (!stop.get()) {
                index.addTablet(new LocalTablet(id++), tabletMeta, true);
                // Keep the list bounded (and exercise the clear path) so the test stays fast.
                if (index.getTablets().size() > 64) {
                    index.clearTabletsForRestore();
                }
            }
        });

        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < 50000 && error.get() == null; i++) {
                    for (Tablet tablet : index.getTablets()) {
                        tablet.getId();
                    }
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                stop.set(true);
            }
        });

        writer.start();
        reader.start();
        reader.join();
        stop.set(true);
        writer.join();

        if (error.get() != null) {
            Assert.fail("getTablets() iteration threw under concurrent mutation: " + error.get());
        }
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        Path path = Files.createFile(Paths.get("./index"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        Text.writeString(dos, GsonUtils.GSON.toJson(index));

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        MaterializedIndex rIndex = GsonUtils.GSON.fromJson(Text.readString(dis), MaterializedIndex.class);
        Assert.assertEquals(index, rIndex);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
