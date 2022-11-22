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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CircleDetectorTest {
    @Test
    public void testCircle() {
        //     0
        //    / \
        //   1   2
        //    \ /
        //     3
        CircleDetector circleDetector = new CircleDetector(4);
        Assertions.assertTrue(circleDetector.tryAddDirectedEdge(0, 1));
        Assertions.assertTrue(circleDetector.tryAddDirectedEdge(1, 3));
        Assertions.assertTrue(circleDetector.tryAddDirectedEdge(3, 2));
        Assertions.assertTrue(!circleDetector.tryAddDirectedEdge(2, 0));
        Assertions.assertTrue(circleDetector.tryAddDirectedEdge(0, 2));
        List<Integer> orders = circleDetector.getTopologicalOrder();
        List<Integer> targetOrders = ImmutableList.of(0, 1, 3, 2);
        assert orders.equals(targetOrders) : orders;
    }
}
