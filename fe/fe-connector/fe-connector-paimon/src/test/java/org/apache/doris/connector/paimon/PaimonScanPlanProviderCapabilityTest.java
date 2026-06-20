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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Guards the predicate-driven scan capability: paimon must opt OUT of the FE prune-to-zero short-circuit
 * ({@link ConnectorScanPlanProvider#ignorePartitionPruneShortCircuit()} == true) so that, with master-parity
 * {@code isNull=false} genuine-null partitions, a {@code col IS NULL} query (which prunes every partition away
 * at FE) is NOT short-circuited to zero rows but re-planned from the pushed predicate — restoring the
 * genuine-null row (regression test_paimon_runtime_filter_partition_pruning qt_null_partition_4). The SPI
 * default stays {@code false} so partition-restricting connectors (e.g. MaxCompute) keep the short-circuit.
 */
public class PaimonScanPlanProviderCapabilityTest {

    @Test
    public void paimonOptsOutOfPruneToZeroShortCircuit() {
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), null);
        Assertions.assertTrue(provider.ignorePartitionPruneShortCircuit(),
                "paimon is predicate-driven and must opt out of the prune-to-zero short-circuit");
    }

    @Test
    public void spiDefaultKeepsShortCircuit() {
        // A connector that does not override the capability keeps the short-circuit (MaxCompute parity).
        ConnectorScanPlanProvider defaultProvider = (session, handle, columns, filter) -> Collections.emptyList();
        Assertions.assertFalse(defaultProvider.ignorePartitionPruneShortCircuit(),
                "the SPI default must keep the prune-to-zero short-circuit");
    }
}
