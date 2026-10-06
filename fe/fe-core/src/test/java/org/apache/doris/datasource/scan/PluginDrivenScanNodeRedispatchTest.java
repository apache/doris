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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.datasource.split.SplitGenerator;
import org.apache.doris.datasource.split.SplitSourceManager;
import org.apache.doris.datasource.split.SplitToScanRange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A plugin-driven scan whose connector planned a range that can be read only once keeps its plan from being
 * dispatched again ({@link PluginDrivenScanNode#cannotBeRedispatched()}), which is what makes
 * StmtExecutor.handleQueryWithRetry refuse to retry a failed attempt with the same plan. An ADBC partition
 * is such a range: a ticket for a result stream of a remote query that already ran, which the failed attempt
 * may have drained, so a retry reading the same tickets returns only what that attempt left -- or nothing --
 * and the query succeeds with rows missing.
 *
 * <p>Driven on a partial ({@code CALLS_REAL_METHODS}) node, as in
 * {@code PluginDrivenScanNodeScanProviderSelectionTest}: every range the node plans goes through
 * {@code toSplit}, on the planning thread and on the batch-mode split generation threads alike.</p>
 */
public class PluginDrivenScanNodeRedispatchTest {

    private static ConnectorScanRange range(boolean singleUse) {
        return new ConnectorScanRange() {
            @Override
            public Optional<String> getPath() {
                return Optional.of("/dummyPath");
            }

            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }

            @Override
            public boolean isSingleUse() {
                return singleUse;
            }
        };
    }

    @Test
    public void planWithSingleUseRangeCannotBeRedispatched() {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Assertions.assertFalse(node.cannotBeRedispatched());

        // Ranges the source serves afresh on every read (a file, a statement run when it is read) keep the
        // same-plan retry.
        Deencapsulation.invoke(node, "toSplit", range(false));
        Assertions.assertFalse(node.cannotBeRedispatched());

        // One range that can be read only once is enough, wherever it comes in the plan.
        Deencapsulation.invoke(node, "toSplit", range(true));
        Assertions.assertTrue(node.cannotBeRedispatched());
        Deencapsulation.invoke(node, "toSplit", range(false));
        Assertions.assertTrue(node.cannotBeRedispatched());
    }

    @Test
    public void batchScanWhoseSplitAssignmentWasStoppedCannotBeRedispatched() {
        // Overriding the method for single-use ranges keeps the rule of every scan with a split assignment
        // (ScanNode#cannotBeRedispatched): a batch-mode scan whose ranges the source serves afresh on every read
        // cannot be dispatched again either once its split assignment was stopped.
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        SplitAssignment assignment = new SplitAssignment(Mockito.mock(FederationBackendPolicy.class),
                Mockito.mock(SplitGenerator.class), Mockito.mock(SplitToScanRange.class), new HashMap<>(),
                new ArrayList<>(), false, new SplitSourceManager());
        Deencapsulation.setField(node, "splitAssignment", assignment);
        Deencapsulation.invoke(node, "toSplit", range(false));
        // Not stopped yet - never dispatched, or failed before anything stopped it: the plan can run again.
        Assertions.assertFalse(node.cannotBeRedispatched());

        // The failed attempt's cancel() stopped it: the split sources the scan ranges point the BE at are gone.
        assignment.stop();
        Assertions.assertTrue(node.cannotBeRedispatched());
    }
}
