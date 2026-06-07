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

package org.apache.doris.connector.maxcompute;

import com.aliyun.odps.PartitionSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * FIX-PRUNE-PUSHDOWN (P4-T06e / DG-1) — guards {@link MaxComputeScanPlanProvider#toPartitionSpecs},
 * the bridge that turns the engine's pruned partition names into ODPS {@link PartitionSpec}s fed to
 * {@code TableReadSessionBuilder.requiredPartitions(...)}.
 *
 * <p><b>Why this matters:</b> this conversion is the ONLY place the pruned partition set reaches the
 * ODPS read session. If it dropped the partitions (e.g. always returned an empty list) the session
 * would span every partition — the exact full-scan regression DG-1 fixes. The null/empty inputs must
 * still mean "read all partitions" so non-pruned queries keep their pre-fix behavior. Mirrors legacy
 * {@code MaxComputeScanNode}'s {@code new PartitionSpec(key)} conversion.</p>
 *
 * <p>The connector module has no fe-core / Mockito; {@code toPartitionSpecs} is a pure static method
 * exercised directly with no network or live ODPS.</p>
 */
public class MaxComputeScanPlanProviderTest {

    @Test
    public void testNullInputMeansScanAll() {
        Assertions.assertTrue(MaxComputeScanPlanProvider.toPartitionSpecs(null).isEmpty());
    }

    @Test
    public void testEmptyInputMeansScanAll() {
        Assertions.assertTrue(
                MaxComputeScanPlanProvider.toPartitionSpecs(Collections.emptyList()).isEmpty());
    }

    @Test
    public void testConvertsPartitionNamesToSpecs() {
        List<PartitionSpec> specs = MaxComputeScanPlanProvider.toPartitionSpecs(
                Arrays.asList("pt=1", "pt=2,region=cn"));

        Assertions.assertEquals(2, specs.size());

        PartitionSpec single = specs.get(0);
        Assertions.assertEquals(Collections.singleton("pt"), single.keys());
        Assertions.assertEquals("1", single.get("pt"));

        PartitionSpec multi = specs.get(1);
        Assertions.assertEquals("2", multi.get("pt"));
        Assertions.assertEquals("cn", multi.get("region"));
    }
}
