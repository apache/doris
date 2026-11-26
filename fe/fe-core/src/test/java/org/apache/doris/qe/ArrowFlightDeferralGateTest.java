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

package org.apache.doris.qe;

import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.planner.ScanNode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

/**
 * The predicate behind the Arrow Flight deferral gate in StmtExecutor.executeAndSendResult (#67503):
 * a coordinator has to outlive GetFlightInfo only when the BE still depends on one of its scans
 * after dispatch - here an external-table scan in batch mode holding a batch split source (#62259).
 * The other reason, a remote Doris scan's Flight SQL session, is covered by RemoteDorisScanNodeTest.
 */
public class ArrowFlightDeferralGateTest {

    private static ScanNode scanNode(boolean batchSplitSource) throws Exception {
        ScanNode node = Mockito.mock(ScanNode.class, Mockito.CALLS_REAL_METHODS);
        if (batchSplitSource) {
            // FileQueryScanNode.createScanRangeLocations sets this only in batch mode.
            Field field = ScanNode.class.getDeclaredField("splitAssignment");
            field.setAccessible(true);
            field.set(node, Mockito.mock(SplitAssignment.class));
        }
        return node;
    }

    @Test
    public void testScanNodeMustOutliveDispatchOnlyWhenSplitsAreHandedOutLazily() throws Exception {
        Assertions.assertFalse(scanNode(false).coordinatorMustOutliveDispatch());
        Assertions.assertTrue(scanNode(true).coordinatorMustOutliveDispatch());
    }

}
