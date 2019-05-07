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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TRepeatNode;

import java.util.*;

/**
 * Used for grouping sets.
 * It will add some new rows and a column of groupingId according to grouping sets info.
 */
public class RepeatNode extends PlanNode {

    private List<Set<Integer>> repeatSlotIdList;
    private TupleDescriptor outputTupleDesc;
    private List<Long> repeatIdValueList;

    public RepeatNode(PlanNodeId id, PlanNode input, List<Set<SlotId>> repeatSlotIdList, TupleDescriptor outputTupleDesc, List<Long> repeatIdValueList) {
        super(id, input.getTupleIds(), "REPEATNODE");
        this.children.add(input);
        this.repeatSlotIdList = buildIdSetList(repeatSlotIdList);
        this.outputTupleDesc = outputTupleDesc;
        this.repeatIdValueList = repeatIdValueList;
        tupleIds.add(outputTupleDesc.getId());
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        avgRowSize = 0;
        cardinality = 0;
        numNodes = 1;
    }

    @Override
    public void init(Analyzer analyzer) {
        Preconditions.checkState(conjuncts.isEmpty());
        for (TupleId id: tupleIds) analyzer.getTupleDesc(id).setIsMaterialized(true);
        computeMemLayout(analyzer);
        computeStats(analyzer);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REPEAT_NODE;
        msg.repeat_node = new TRepeatNode(outputTupleDesc.getId().asInt(), repeatSlotIdList, repeatIdValueList);
    }

    @Override
    protected String debugString() {
        return Objects.toStringHelper(this).add("Repeat", repeatSlotIdList.size()).addValue(
                super.debugString()).toString();
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(detailPrefix + "repeat: repeat ");
        output.append(repeatSlotIdList.size() - 1);
        output.append(" lines and new add 1 column\n");
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }

    public static List<Long> convertToLongList(List<BitSet> bitSetList) {
        List<Long> groupingIdList = new ArrayList<>();
        for(BitSet bitSet: bitSetList) {
            long l = 0L;
            for (int i = 0; i < bitSet.length(); ++i) {
                l += bitSet.get(i) ? (1L << i) : 0L;
            }
            groupingIdList.add(l);
        }
        return groupingIdList;
    }

    private List<List<Boolean>> convertToBooleanList(List<BitSet> bitSetList) {
        List<List<Boolean>> groupingIdList = new ArrayList<>();
        for(BitSet bitSet: bitSetList) {
            List<Boolean> l = new ArrayList<>();
            for (int i = 0; i < outputTupleDesc.getSlots().size(); ++i) {
                l.add(bitSet.get(i));
            }
            groupingIdList.add(l);
        }
        return groupingIdList;
    }

    public static long convert(BitSet bits) {
        long value = 0L;
        for (int i = 0; i < bits.length(); ++i) {
            value += bits.get(i) ? (1L << i) : 0L;
        }
        return value;
    }

    private static List<Set<Integer>> buildIdSetList(List<Set<SlotId>> repeatSlotIdList) {
        List<Set<Integer>> slotIdList = new ArrayList<>();
        for(Set slotSet : repeatSlotIdList) {
            Set<Integer> intSet = new HashSet<>();
            for(Object slotId : slotSet) {
                intSet.add(((SlotId)slotId).asInt());
            }
            slotIdList.add(intSet);
        }
        return slotIdList;
    }
}
