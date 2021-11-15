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

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class RepeatNodeTest {
    private Analyzer analyzer;
    private RepeatNode node;
    private TupleDescriptor virtualTuple;
    private List<Set<SlotId>> groupingIdList = new ArrayList<>();
    private List<List<Long>> groupingList = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        Analyzer analyzerBase = AccessTestUtil.fetchTableAnalyzer();
        analyzer = new Analyzer(analyzerBase.getCatalog(), analyzerBase.getContext());
        String[] cols = {"k1", "k2", "k3"};
        List<SlotRef> slots = new ArrayList<>();
        for (String col : cols) {
            SlotRef expr = new SlotRef(new TableName("testdb", "t"), col);
            slots.add(expr);
        }
        try {
            Field f = analyzer.getClass().getDeclaredField("tupleByAlias");
            f.setAccessible(true);
            Multimap<String, TupleDescriptor> tupleByAlias = ArrayListMultimap.create();
            TupleDescriptor td = new TupleDescriptor(new TupleId(0));
            td.setTable(analyzerBase.getTableOrAnalysisException(new TableName("testdb", "t")));
            tupleByAlias.put("testdb.t", td);
            f.set(analyzer, tupleByAlias);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        virtualTuple = analyzer.getDescTbl().createTupleDescriptor("VIRTUAL_TUPLE");
        groupingList.add(Arrays.asList(0L, 7L, 3L, 5L, 1L, 6L, 2L, 4L));
        groupingList.add(Arrays.asList(0L, 7L, 3L, 5L, 1L, 6L, 2L, 4L));
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tuple = descTable.createTupleDescriptor("DstTable");
        node = new RepeatNode(new PlanNodeId(1),
                new OlapScanNode(new PlanNodeId(0), tuple, "null"), groupingIdList, virtualTuple, groupingList);

    }

    @Test
    public void testNornal() {
        try {
            TPlanNode msg = new TPlanNode();
            node.toThrift(msg);
            node.getNodeExplainString("", TExplainLevel.NORMAL);
            node.debugString();
            Assert.assertEquals(TPlanNodeType.REPEAT_NODE, msg.node_type);
        } catch (Exception e) {
            Assert.fail("throw exceptions");
        }
    }
}
