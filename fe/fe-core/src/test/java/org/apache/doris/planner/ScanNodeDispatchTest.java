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

package org.apache.doris.planner;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

/**
 * A scan node's part in running its plan: the coordinator dispatching the plan starts the split assignment the BE
 * fetches the splits from, and stops it when it closes or cancels.
 */
public class ScanNodeDispatchTest {

    private static ScanNode scanNode(SplitAssignment splitAssignment) throws Exception {
        ScanNode node = Mockito.mock(ScanNode.class, Mockito.CALLS_REAL_METHODS);
        // FileQueryScanNode.createScanRangeLocations sets this only in batch mode.
        Field field = ScanNode.class.getDeclaredField("splitAssignment");
        field.setAccessible(true);
        field.set(node, splitAssignment);
        return node;
    }

    @Test
    public void testStartAndStopDriveTheSplitAssignment() throws Exception {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        ScanNode node = scanNode(assignment);

        node.start();
        Mockito.verify(assignment).start();
        node.stop();
        Mockito.verify(assignment).stop();

        // A scan whose ranges carry everything the BE scans with has nothing to start or stop.
        ScanNode plain = scanNode(null);
        Assertions.assertDoesNotThrow(plain::start);
        Assertions.assertDoesNotThrow(plain::stop);
    }

    @Test
    public void testStopAllStopsTheNodesAfterOneThatFailsToStop() {
        // A split assignment rethrows the failure of its asynchronous split generation from stop().
        ScanNode failing = Mockito.mock(ScanNode.class);
        Mockito.doThrow(new RuntimeException("split generation failed")).when(failing).stop();
        ScanNode next = Mockito.mock(ScanNode.class);

        Assertions.assertDoesNotThrow(
                () -> ScanNode.stopAll(Lists.newArrayList(failing, next), new TUniqueId(1L, 2L)));

        Mockito.verify(next).stop();
    }

    @Test
    public void testStartAllPropagatesTheFirstFailure() throws Exception {
        ScanNode failing = Mockito.mock(ScanNode.class);
        Mockito.doThrow(new UserException("remote query failed")).when(failing).start();
        ScanNode next = Mockito.mock(ScanNode.class);

        UserException e = Assertions.assertThrows(UserException.class,
                () -> ScanNode.startAll(Lists.newArrayList(failing, next)));

        Assertions.assertTrue(e.getMessage().contains("remote query failed"), e.getMessage());
        // The caller cancels or closes the query then, which stops the nodes; the rest are not started meanwhile.
        Mockito.verify(next, Mockito.never()).start();
    }
}
