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

package org.apache.doris.common.profile;

import org.apache.doris.thrift.TCounter;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

@ResourceLock("global")
public class RuntimeProfileMergeTest {
    private static final Logger LOG = LogManager.getLogger(RuntimeProfileMergeTest.class);

    @Test
    public void testMergeCounter() {
        RuntimeProfile profile1 = new RuntimeProfile("profile1");
        RuntimeProfile profile2 = new RuntimeProfile("profile2");

        Counter counter1 = profile1.addCounter("Counter1", TUnit.UNIT, RuntimeProfile.ROOT_COUNTER);
        Counter counter2 = profile1.addCounter("Counter2", TUnit.UNIT, RuntimeProfile.ROOT_COUNTER);
        counter1.setValue(101);
        counter2.setValue(102);
        Counter counter1Child1 = profile1.addCounter("Counter1_Child1", TUnit.UNIT, "Counter1");
        Counter counter2Child1 = profile1.addCounter("Counter2_Child1", TUnit.UNIT, "Counter2");
        counter1Child1.setValue(1011);
        counter2Child1.setValue(1021);
        counter1.setLevel(1);
        counter2.setLevel(1);
        counter1Child1.setLevel(1);
        counter2Child1.setLevel(1);

        profile2.addCounter("Counter1", counter1, RuntimeProfile.ROOT_COUNTER);
        profile2.addCounter("Counter2", counter2, RuntimeProfile.ROOT_COUNTER);

        RuntimeProfile mergeProfile = new RuntimeProfile("mergeProfile");
        RuntimeProfile.mergeCounters(RuntimeProfile.ROOT_COUNTER, Lists.newArrayList(profile1, profile2), mergeProfile);
        /*
        mergeProfile:
           - Counter1: sum 202, avg 101, max 101, min 101
             - Counter1_Child1: sum 1.011K (1011), avg 1.011K (1011), max 1.011K (1011), min 1.011K (1011)
           - Counter2: sum 204, avg 102, max 102, min 102
             - Counter2_Child1: sum 1.021K (1021), avg 1.021K (1021), max 1.021K (1021), min 1.021K (1021)
        * */
        LOG.info("Profile1:\n{}", mergeProfile.toString());

        Assert.assertTrue(mergeProfile.getCounterMap().get("Counter1") instanceof AggCounter);
        AggCounter aggCounter = (AggCounter) mergeProfile.getCounterMap().get("Counter1");
        Assert.assertEquals(aggCounter.sum.getValue(), 202);
        Assert.assertEquals(aggCounter.number, 2);
    }

    @Test
    public void testMergeProfileNormal() {
        TRuntimeProfileTree tRuntimeProfileTree = new TRuntimeProfileTree();
        TRuntimeProfileNode rootNode = new TRuntimeProfileNode();
        rootNode.name = "rootNode";
        rootNode.num_children = 2;
        tRuntimeProfileTree.addToNodes(rootNode);
        TRuntimeProfileNode node1 = new TRuntimeProfileNode();
        TRuntimeProfileNode node2 = new TRuntimeProfileNode();
        node1.name = "XXX_OPERATOR(nereids_id=111)";
        node2.name = "CommonCounters";
        node1.num_children = 0;
        node2.num_children = 0;
        tRuntimeProfileTree.addToNodes(node1);
        tRuntimeProfileTree.addToNodes(node2);

        node1.child_counters_map = new HashMap<>();
        node1.counters = new ArrayList<>();
        node2.child_counters_map = new HashMap<>();
        node2.counters = new ArrayList<>();

        TCounter counter1 = new TCounter("Counter1", TUnit.UNIT, 1);
        TCounter counter2 = new TCounter("Counter2", TUnit.UNIT, 1);
        TCounter counter1Child = new TCounter("Counter1-Child1", TUnit.UNIT, 1);
        TCounter counter2Child = new TCounter("Counter2-Child1", TUnit.UNIT, 1);
        // Set counter-level to 1 so that the counter could be merged.
        counter1.setLevel(1);
        counter2.setLevel(1);
        counter1Child.setLevel(1);
        counter2Child.setLevel(1);
        HashMap<String, Set<String>> childCountersMap = new HashMap<>();
        childCountersMap.put("", Sets.newHashSet("Counter1", "Counter2"));
        childCountersMap.put("Counter1", Sets.newHashSet("Counter1-Child1"));
        childCountersMap.put("Counter2", Sets.newHashSet("Counter2-Child1"));

        node1.counters.add(counter1);
        node1.counters.add(counter2);
        node1.counters.add(counter1Child);
        node1.counters.add(counter2Child);

        node2.counters.add(counter1);
        node2.counters.add(counter2);
        node2.counters.add(counter1Child);
        node2.counters.add(counter2Child);

        node1.child_counters_map = childCountersMap;
        node2.child_counters_map = childCountersMap;

        // Each RuntimeProfile has a Counter named "TotalTime" after it is created.
        RuntimeProfile profile1 = new RuntimeProfile("profile1");
        RuntimeProfile profile2 = new RuntimeProfile("profile2");
        RuntimeProfile profile3 = new RuntimeProfile("profile3");
        /*
        profile1:
            node1:
               - Counter1: 1
                 - Counter1-Child1: 1
               - Counter2: 1
                 - Counter2-Child1: 1
            node2:
               - Counter1: 1
                 - Counter1-Child1: 1
               - Counter2: 1
                 - Counter2-Child1: 1
        * */
        profile1.update(tRuntimeProfileTree);
        profile2.update(tRuntimeProfileTree);
        profile3.update(tRuntimeProfileTree);

        LOG.info("Profile1:\n{}", profile1.toString());

        /*
        *
        mergedProfile:
              node1:
                 - Counter1: sum 3, avg 1, max 1, min 1
                   - Counter1-Child1: sum 3, avg 1, max 1, min 1
                 - Counter2: sum 3, avg 1, max 1, min 1
                   - Counter2-Child1: sum 3, avg 1, max 1, min 1
              node2:
                 - Counter1: sum 3, avg 1, max 1, min 1
                   - Counter1-Child1: sum 3, avg 1, max 1, min 1
                 - Counter2: sum 3, avg 1, max 1, min 1
                   - Counter2-Child1: sum 3, avg 1, max 1, min 1
        * */
        RuntimeProfile mergedProfile = new RuntimeProfile("mergedProfile");
        RuntimeProfile.mergeProfiles(Lists.newArrayList(profile1, profile2, profile3), mergedProfile, null);

        StringBuilder builder = new StringBuilder();
        mergedProfile.prettyPrint(builder, "\t");
        LOG.info("Merged profile:\n{}", builder.toString());

        Assert.assertEquals(mergedProfile.getChildList().size(), 2);
        Assert.assertTrue(
                mergedProfile.getChildList().get(0).first.getCounterMap().get("Counter1") instanceof AggCounter);
        AggCounter aggCounterNode1 = (AggCounter) mergedProfile.getChildList().get(0).first.getCounterMap()
                .get("Counter1");
        Assert.assertEquals(aggCounterNode1.sum.getValue(), 3);
        Assert.assertEquals(aggCounterNode1.number, 3);
    }

    // Test the case where counter of RuntimeProfile has different structure.
    // When non-ZeroCounter is involved, counter-structure of RuntimeProfile is different.
    @Test
    public void testMergeProfileWithDifferentCounter() {
        /*
        profile1:
            node1:
               - Counter1: 1
                 - Counter1-Child1: 1
               - Counter2: 1
                 - Counter2-Child1: 1
            node2:
               - Counter1: 1
                 - Counter1-Child1: 1
               - Counter2: 1
                 - Counter2-Child1: 1
        * */
        TRuntimeProfileTree tRuntimeProfileTree1 = new TRuntimeProfileTree();
        TRuntimeProfileNode rootNode = new TRuntimeProfileNode();
        rootNode.name = "rootNode";
        rootNode.num_children = 2;
        tRuntimeProfileTree1.addToNodes(rootNode);
        TRuntimeProfileNode node1 = new TRuntimeProfileNode();
        TRuntimeProfileNode node2 = new TRuntimeProfileNode();
        node1.name = "node1";
        node2.name = "node2";
        node1.num_children = 0;
        node2.num_children = 0;
        tRuntimeProfileTree1.addToNodes(node1);
        tRuntimeProfileTree1.addToNodes(node2);

        node1.child_counters_map = new HashMap<>();
        node1.counters = new ArrayList<>();
        node2.child_counters_map = new HashMap<>();
        node2.counters = new ArrayList<>();

        TCounter counter1 = new TCounter("Counter1", TUnit.UNIT, 1);
        TCounter counter2 = new TCounter("Counter2", TUnit.UNIT, 1);
        TCounter counter1Child = new TCounter("Counter1-Child1", TUnit.UNIT, 1);
        TCounter counter2Child = new TCounter("Counter2-Child1", TUnit.UNIT, 1);
        // Set counter-level to 1 so that the counter could be merged.
        counter1.setLevel(1);
        counter2.setLevel(1);
        counter1Child.setLevel(1);
        counter2Child.setLevel(1);
        HashMap<String, Set<String>> childCountersMap = new HashMap<>();
        childCountersMap.put("", Sets.newHashSet("Counter1", "Counter2"));
        childCountersMap.put("Counter1", Sets.newHashSet("Counter1-Child1"));
        childCountersMap.put("Counter2", Sets.newHashSet("Counter2-Child1"));

        node1.counters.addAll(Lists.newArrayList(counter1, counter2, counter1Child, counter2Child));
        node1.child_counters_map = childCountersMap;
        node2.counters.addAll(Lists.newArrayList(counter1, counter2, counter1Child, counter2Child));
        node2.child_counters_map = childCountersMap;
        RuntimeProfile profile1 = new RuntimeProfile("profile1");
        profile1.update(tRuntimeProfileTree1);

        /*
        profile1:
            node1:
               - Counter2: 1
                 - Counter2-Child1: 1
            node2:
               - Counter1: 1
                 - Counter1-Child1: 1
        * */
        TRuntimeProfileTree tRuntimeProfileTree2 = new TRuntimeProfileTree();
        TRuntimeProfileNode rootNode2 = new TRuntimeProfileNode();
        rootNode2.name = "rootNode";
        rootNode2.num_children = 2;
        tRuntimeProfileTree2.addToNodes(rootNode2);

        node1.counters.clear();
        node2.counters.clear();

        node1.counters.addAll(Lists.newArrayList(counter2, counter2Child));
        node2.counters.addAll(Lists.newArrayList(counter1, counter1Child));

        node1.child_counters_map = new HashMap<>();
        node1.child_counters_map.put("", Sets.newHashSet("Counter2"));
        node1.child_counters_map.put("Counter2", Sets.newHashSet("Counter2-Child1"));

        node2.child_counters_map = new HashMap<>();
        node2.child_counters_map.put("", Sets.newHashSet("Counter1"));
        node2.child_counters_map.put("Counter1", Sets.newHashSet("Counter1-Child1"));

        tRuntimeProfileTree2.addToNodes(node1);
        tRuntimeProfileTree2.addToNodes(node2);

        RuntimeProfile profile2 = new RuntimeProfile("profile2");
        profile2.update(tRuntimeProfileTree2);

        // Let's merge them.
        // Profile 1 and profile 2 have different counter-structure.
        // But they can still do merge.
        RuntimeProfile mergedProfile = new RuntimeProfile("mergedProfile");
        RuntimeProfile.mergeProfiles(Lists.newArrayList(profile1, profile2), mergedProfile, null);

        StringBuilder builder = new StringBuilder();
        mergedProfile.prettyPrint(builder, "\t");
        LOG.info("Merged profile:\n{}", builder.toString());

        /*
        *
        mergedProfile:
          node1:
             - Counter1: sum 1, avg 1, max 1, min 1
               - Counter1-Child1: sum 1, avg 1, max 1, min 1
             - Counter2: sum 2, avg 1, max 1, min 1
               - Counter2-Child1: sum 2, avg 1, max 1, min 1
          node2:
             - Counter1: sum 2, avg 1, max 1, min 1
               - Counter1-Child1: sum 2, avg 1, max 1, min 1
             - Counter2: sum 1, avg 1, max 1, min 1
               - Counter2-Child1: sum 1, avg 1, max 1, min 1
        *
        * */
    }
}
