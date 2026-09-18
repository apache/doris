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

import org.apache.doris.planner.OlapScanNode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PointQueryExecutorTest {
    @Test
    public void testCandidateBackendsShuffleDependsOnQuerySelectionOrder() {
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);

        Mockito.when(scanNode.isScanBackendOrderBySelection()).thenReturn(false);
        Assert.assertTrue(PointQueryExecutor.shouldShuffleCandidateBackends(scanNode));

        Mockito.when(scanNode.isScanBackendOrderBySelection()).thenReturn(true);
        Assert.assertFalse(PointQueryExecutor.shouldShuffleCandidateBackends(scanNode));
    }
}
