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

package org.apache.doris.backup;

import org.apache.doris.backup.MediumDecisionMaker.MediumDecision;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.DataProperty.MediumAllocationMode;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MediumDecisionMakerTest {

    @Mocked
    private Env env;

    @Mocked
    private SystemInfoService systemInfoService;

    @Mocked
    private OlapTable olapTable;

    private ReplicaAllocation replicaAlloc;
    private DataProperty ssdDataProperty;
    private DataProperty hddDataProperty;

    @Before
    public void setUp() {
        replicaAlloc = new ReplicaAllocation((short) 3);
        ssdDataProperty = new DataProperty(TStorageMedium.SSD);
        hddDataProperty = new DataProperty(TStorageMedium.HDD);
    }

    @Test
    public void testMediumDecisionBasicProperties() {
        MediumDecision decision = new MediumDecision(
                TStorageMedium.SSD,
                TStorageMedium.SSD,
                false,
                "test reason"
        );

        Assert.assertEquals(TStorageMedium.SSD, decision.getFinalMedium());
        Assert.assertEquals(TStorageMedium.SSD, decision.getOriginalMedium());
        Assert.assertFalse(decision.wasDowngraded());
        Assert.assertEquals("test reason", decision.getReason());
    }

    @Test
    public void testMediumDecisionWithDowngrade() {
        MediumDecision decision = new MediumDecision(
                TStorageMedium.HDD,
                TStorageMedium.SSD,
                true,
                "downgraded from SSD to HDD"
        );

        Assert.assertEquals(TStorageMedium.HDD, decision.getFinalMedium());
        Assert.assertEquals(TStorageMedium.SSD, decision.getOriginalMedium());
        Assert.assertTrue(decision.wasDowngraded());
        Assert.assertTrue(decision.getReason().contains("downgraded"));
    }

    @Test
    public void testMediumDecisionToString() {
        MediumDecision decision = new MediumDecision(
                TStorageMedium.SSD,
                TStorageMedium.HDD,
                true,
                "test reason"
        );

        String str = decision.toString();
        Assert.assertTrue(str.contains("MediumDecision"));
        Assert.assertTrue(str.contains("SSD"));
        Assert.assertTrue(str.contains("HDD"));
        Assert.assertTrue(str.contains("true"));
        Assert.assertTrue(str.contains("test reason"));
    }

    @Test
    public void testConstructorWithHddMedium() {
        MediumDecisionMaker maker = new MediumDecisionMaker("hdd", "strict");
        // Verify it's created without exception
        Assert.assertNotNull(maker);
    }

    @Test
    public void testConstructorWithSsdMedium() {
        MediumDecisionMaker maker = new MediumDecisionMaker("ssd", "adaptive");
        Assert.assertNotNull(maker);
    }

    @Test
    public void testConstructorWithSameWithUpstream() {
        MediumDecisionMaker maker = new MediumDecisionMaker("same_with_upstream", "strict");
        Assert.assertNotNull(maker);
    }

    @Test
    public void testConstructorWithNullValues() {
        // Should not throw exception, null values are handled
        MediumDecisionMaker maker = new MediumDecisionMaker(null, null);
        Assert.assertNotNull(maker);
    }

    @Test
    public void testDecideForTableLevelWithSameWithUpstream() {
        new Expectations() {
            {
                olapTable.getStorageMedium();
                result = TStorageMedium.SSD;

                // getName() is only called when LOG.isDebugEnabled()
                olapTable.getName();
                minTimes = 0;
                result = "test_table";
            }
        };

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT
        );

        TStorageMedium result = maker.decideForTableLevel(olapTable);
        Assert.assertEquals(TStorageMedium.SSD, result);
    }

    @Test
    public void testDecideForTableLevelWithSameWithUpstreamHdd() {
        new Expectations() {
            {
                olapTable.getStorageMedium();
                result = TStorageMedium.HDD;

                // getName() is only called when LOG.isDebugEnabled()
                olapTable.getName();
                minTimes = 0;
                result = "test_table";
            }
        };

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_ADAPTIVE
        );

        TStorageMedium result = maker.decideForTableLevel(olapTable);
        Assert.assertEquals(TStorageMedium.HDD, result);
    }

    @Test
    public void testDecideForTableLevelWithExplicitHdd() {
        new Expectations() {
            {
                // getName() is only called when LOG.isDebugEnabled()
                olapTable.getName();
                minTimes = 0;
                result = "test_table";
            }
        };

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_HDD,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT
        );

        TStorageMedium result = maker.decideForTableLevel(olapTable);
        Assert.assertEquals(TStorageMedium.HDD, result);
    }

    @Test
    public void testDecideForTableLevelWithExplicitSsd() {
        new Expectations() {
            {
                // getName() is only called when LOG.isDebugEnabled()
                olapTable.getName();
                minTimes = 0;
                result = "test_table";
            }
        };

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SSD,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_ADAPTIVE
        );

        TStorageMedium result = maker.decideForTableLevel(olapTable);
        Assert.assertEquals(TStorageMedium.SSD, result);
    }

    @Test
    public void testDecideForNewPartitionWithSameWithUpstream() throws DdlException {
        setupSystemInfoServiceMock(TStorageMedium.SSD, TStorageMedium.SSD);

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT
        );

        MediumDecision decision = maker.decideForNewPartition("p1", ssdDataProperty, replicaAlloc);

        Assert.assertEquals(TStorageMedium.SSD, decision.getFinalMedium());
        Assert.assertEquals(TStorageMedium.SSD, decision.getOriginalMedium());
        Assert.assertFalse(decision.wasDowngraded());
        Assert.assertTrue(decision.getReason().contains("inherited from upstream"));
    }

    @Test
    public void testDecideForNewPartitionWithExplicitHdd() throws DdlException {
        setupSystemInfoServiceMock(TStorageMedium.HDD, TStorageMedium.HDD);

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_HDD,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT
        );

        MediumDecision decision = maker.decideForNewPartition("p1", ssdDataProperty, replicaAlloc);

        Assert.assertEquals(TStorageMedium.HDD, decision.getFinalMedium());
        Assert.assertEquals(TStorageMedium.HDD, decision.getOriginalMedium());
        Assert.assertFalse(decision.wasDowngraded());
        Assert.assertTrue(decision.getReason().contains("user specified"));
    }

    @Test
    public void testDecideForNewPartitionWithDowngrade() throws DdlException {
        // Simulate SSD unavailable, downgrade to HDD
        setupSystemInfoServiceMock(TStorageMedium.SSD, TStorageMedium.HDD);

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SSD,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_ADAPTIVE
        );

        MediumDecision decision = maker.decideForNewPartition("p1", hddDataProperty, replicaAlloc);

        Assert.assertEquals(TStorageMedium.HDD, decision.getFinalMedium());
        Assert.assertEquals(TStorageMedium.SSD, decision.getOriginalMedium());
        Assert.assertTrue(decision.wasDowngraded());
        Assert.assertTrue(decision.getReason().contains("downgraded"));
    }

    @Test
    public void testDecideForAtomicRestoreWithSameWithUpstreamStrictMode() throws DdlException {
        // Setup: same_with_upstream + strict mode
        // Local medium is HDD, upstream is SSD
        // Strict mode should use local medium (HDD) to avoid migration
        setupSystemInfoServiceMock(TStorageMedium.HDD, TStorageMedium.HDD);

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT
        );

        MediumDecision decision = maker.decideForAtomicRestore(
                "p1", ssdDataProperty, hddDataProperty, replicaAlloc);

        Assert.assertEquals(TStorageMedium.HDD, decision.getFinalMedium());
        Assert.assertTrue(decision.getReason().contains("strict mode"));
        Assert.assertTrue(decision.getReason().contains("local medium"));
    }

    @Test
    public void testDecideForAtomicRestoreWithSameWithUpstreamAdaptiveLocalAvailable() throws DdlException {
        // Setup: same_with_upstream + adaptive mode
        // Local medium (SSD) is available
        setupSystemInfoServiceMockForCheck(TStorageMedium.SSD, TStorageMedium.SSD);

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_ADAPTIVE
        );

        MediumDecision decision = maker.decideForAtomicRestore(
                "p1", ssdDataProperty, ssdDataProperty, replicaAlloc);

        Assert.assertEquals(TStorageMedium.SSD, decision.getFinalMedium());
        Assert.assertTrue(decision.getReason().contains("prefer local"));
    }

    @Test
    public void testDecideForAtomicRestoreWithSameWithUpstreamAdaptiveLocalUnavailable() throws DdlException {
        // Setup: same_with_upstream + adaptive mode
        // Local medium (SSD) is unavailable, should downgrade to HDD
        setupSystemInfoServiceMockForCheck(TStorageMedium.SSD, TStorageMedium.HDD);

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_ADAPTIVE
        );

        MediumDecision decision = maker.decideForAtomicRestore(
                "p1", ssdDataProperty, ssdDataProperty, replicaAlloc);

        Assert.assertEquals(TStorageMedium.HDD, decision.getFinalMedium());
        Assert.assertTrue(decision.wasDowngraded());
        Assert.assertTrue(decision.getReason().contains("unavailable"));
    }

    @Test
    public void testDecideForAtomicRestoreWithExplicitMedium() throws DdlException {
        // Setup: explicit SSD (non same_with_upstream)
        // This should use configured medium, allowing migration
        setupSystemInfoServiceMock(TStorageMedium.SSD, TStorageMedium.SSD);

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SSD,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT
        );

        MediumDecision decision = maker.decideForAtomicRestore(
                "p1", hddDataProperty, hddDataProperty, replicaAlloc);

        Assert.assertEquals(TStorageMedium.SSD, decision.getFinalMedium());
        Assert.assertTrue(decision.getReason().contains("explicit medium"));
        Assert.assertTrue(decision.getReason().contains("migration allowed"));
    }

    @Test
    public void testDecideForAtomicRestoreWithExplicitMediumDowngrade() throws DdlException {
        // Setup: explicit SSD but unavailable, adaptive mode should downgrade
        setupSystemInfoServiceMock(TStorageMedium.SSD, TStorageMedium.HDD);

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_SSD,
                RestoreCommand.MEDIUM_ALLOCATION_MODE_ADAPTIVE
        );

        MediumDecision decision = maker.decideForAtomicRestore(
                "p1", hddDataProperty, hddDataProperty, replicaAlloc);

        Assert.assertEquals(TStorageMedium.HDD, decision.getFinalMedium());
        Assert.assertTrue(decision.wasDowngraded());
    }

    private void setupSystemInfoServiceMock(TStorageMedium requestedMedium, TStorageMedium returnedMedium)
            throws DdlException {
        new Expectations() {
            {
                Env.getCurrentSystemInfo();
                result = systemInfoService;

                systemInfoService.selectBackendIdsForReplicaCreation(
                        (ReplicaAllocation) any, (Map<Tag, Integer>) any,
                        (TStorageMedium) any, (MediumAllocationMode) any, anyBoolean);
                result = new Delegate<Pair<Map<Tag, List<Long>>, TStorageMedium>>() {
                    @SuppressWarnings("unused")
                    public Pair<Map<Tag, List<Long>>, TStorageMedium> delegate(
                            ReplicaAllocation replicaAlloc, Map<Tag, Integer> nextIndexs,
                            TStorageMedium medium, MediumAllocationMode mode, boolean isOnlyForCheck) {
                        Map<Tag, List<Long>> backendIds = new HashMap<>();
                        backendIds.put(Tag.DEFAULT_BACKEND_TAG, Lists.newArrayList(1L, 2L, 3L));
                        return Pair.of(backendIds, returnedMedium);
                    }
                };
            }
        };
    }

    private void setupSystemInfoServiceMockForCheck(TStorageMedium requestedMedium, TStorageMedium returnedMedium)
            throws DdlException {
        new Expectations() {
            {
                Env.getCurrentSystemInfo();
                result = systemInfoService;

                systemInfoService.selectBackendIdsForReplicaCreation(
                        (ReplicaAllocation) any, (Map<Tag, Integer>) any,
                        (TStorageMedium) any, (MediumAllocationMode) any, anyBoolean);
                result = new Delegate<Pair<Map<Tag, List<Long>>, TStorageMedium>>() {
                    @SuppressWarnings("unused")
                    public Pair<Map<Tag, List<Long>>, TStorageMedium> delegate(
                            ReplicaAllocation replicaAlloc, Map<Tag, Integer> nextIndexs,
                            TStorageMedium medium, MediumAllocationMode mode, boolean isOnlyForCheck) {
                        Map<Tag, List<Long>> backendIds = new HashMap<>();
                        backendIds.put(Tag.DEFAULT_BACKEND_TAG, Lists.newArrayList(1L, 2L, 3L));
                        return Pair.of(backendIds, returnedMedium);
                    }
                };
            }
        };
    }

    @Test
    public void testDecideForNewPartitionWithNullAllocationMode() throws DdlException {
        // Test with null allocation mode - should default to STRICT
        setupSystemInfoServiceMock(TStorageMedium.HDD, TStorageMedium.HDD);

        MediumDecisionMaker maker = new MediumDecisionMaker(
                RestoreCommand.STORAGE_MEDIUM_HDD,
                null  // null allocation mode
        );

        MediumDecision decision = maker.decideForNewPartition("p1", ssdDataProperty, replicaAlloc);
        Assert.assertEquals(TStorageMedium.HDD, decision.getFinalMedium());
    }
}

