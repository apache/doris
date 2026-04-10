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

package org.apache.doris.load;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeleteJobTest {
    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 2L;
    private static final long PARTITION_ID = 3L;
    private static final long TABLET_ID = 4L;

    private final Database database = new Database(DB_ID, "db");
    private final InternalCatalog catalog = Deencapsulation.newInstance(InternalCatalog.class);
    private final TabletInvertedIndex invertedIndex = new TabletInvertedIndex() {
        @Override
        public List<Replica> getReplicas(Long tabletId) {
            return Lists.newArrayList();
        }

        @Override
        public void deleteTablet(long tabletId) {
        }

        @Override
        public void addReplica(long tabletId, Replica replica) {
        }

        @Override
        public void deleteReplica(long tabletId, long backendId) {
        }

        @Override
        public Replica getReplica(long tabletId, long backendId) {
            return null;
        }

        @Override
        public List<Replica> getReplicasByTabletId(long tabletId) {
            return Lists.newArrayList();
        }

        @Override
        protected void innerClear() {
        }
    };

    @Before
    public void setUp() {
        invertedIndex.addTablet(TABLET_ID, new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, 5L, 6, TStorageMedium.HDD));

        new MockUp<Env>() {
            @Mock
            public InternalCatalog getCurrentInternalCatalog() {
                return catalog;
            }

            @Mock
            public TabletInvertedIndex getCurrentInvertedIndex() {
                return invertedIndex;
            }
        };

        new MockUp<InternalCatalog>() {
            @Mock
            public Database getDbOrMetaException(long dbId) {
                return database;
            }
        };
    }

    @Test
    public void testAwaitIgnoresFailureStatusAfterQuorumReached() throws Exception {
        MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(1);
        latch.addMark(1L, TABLET_ID);
        latch.markedCountDownWithStatus(1L, TABLET_ID, new Status(TStatusCode.INTERNAL_ERROR, "too many versions"));

        DeleteJob deleteJob = newDeleteJob((short) 3, latch);
        deleteJob.addFinishedReplica(PARTITION_ID, TABLET_ID,
                new LocalReplica(11L, 21L, Replica.ReplicaState.NORMAL, 1L, 6));
        deleteJob.addFinishedReplica(PARTITION_ID, TABLET_ID,
                new LocalReplica(12L, 22L, Replica.ReplicaState.NORMAL, 1L, 6));

        deleteJob.await();

        Assert.assertEquals(DeleteJob.DeleteState.QUORUM_FINISHED, deleteJob.getState());
    }

    @Test
    public void testAwaitReturnsFailureStatusWhenQuorumNotReached() {
        MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(1);
        latch.addMark(1L, TABLET_ID);
        latch.markedCountDownWithStatus(1L, TABLET_ID, new Status(TStatusCode.INTERNAL_ERROR, "too many versions"));

        DeleteJob deleteJob = newDeleteJob((short) 1, latch);

        try {
            deleteJob.await();
            Assert.fail("delete job should fail when quorum is not reached");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("too many versions"));
            Assert.assertEquals(DeleteJob.DeleteState.UN_QUORUM, deleteJob.getState());
        } catch (Exception e) {
            Assert.fail("unexpected exception: " + e.getMessage());
        }
    }

    private DeleteJob newDeleteJob(short replicaNum, MarkedCountDownLatch<Long, Long> latch) {
        DeleteInfo deleteInfo = new DeleteInfo(DB_ID, TABLE_ID, "tbl", Collections.emptyList(),
                false, Lists.newArrayList(PARTITION_ID), Lists.newArrayList("p1"));
        Map<Long, Short> partitionReplicaNum = Collections.singletonMap(PARTITION_ID, replicaNum);
        DeleteJob deleteJob = new DeleteJob(100L, 200L, "label", partitionReplicaNum, deleteInfo);
        deleteJob.setCountDownLatch(latch);
        Set<Long> totalTablets = Deencapsulation.getField(deleteJob, "totalTablets");
        totalTablets.add(TABLET_ID);
        return deleteJob;
    }
}
