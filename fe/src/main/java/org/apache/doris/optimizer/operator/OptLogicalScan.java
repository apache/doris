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

package org.apache.doris.optimizer.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.doris.optimizer.stat.StatisticsEstimator;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class OptLogicalScan extends OptLogical {

    private List<OptColumnRef> outputs;
    private Map<Integer, SlotDescriptor> idSlotMap;
    private BaseTableRef table;

    public OptLogicalScan() {
        this(null);
    }

    public OptLogicalScan(BaseTableRef table) {
        super(OptOperatorType.OP_LOGICAL_SCAN);
        this.outputs = Lists.newArrayList();
        this.table = table;
        this.idSlotMap = Maps.newHashMap();
        createOutputColumns();
    }

    private void createOutputColumns() {
        for (SlotDescriptor slot : table.getDesc().getMaterializedSlots()) {
            final OptColumnRef columnRef = new OptColumnRef(slot.getId().asInt(), slot.getType(), slot.getColumn().getName());
            outputs.add(columnRef);
            idSlotMap.put(slot.getId().asInt(), slot);
        }
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        final BitSet set = new BitSet();
        set.set(OptRuleType.RULE_IMP_OLAP_LSCAN_TO_PSCAN.ordinal());
        return set;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        return new OptColumnRefSet(outputs);
    }

    @Override
    public Statistics deriveStat(OptExpressionHandle exprHandle, RequiredLogicalProperty property) {
        return StatisticsEstimator.estimateOlapScan(table, property);
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle expressionHandle,
            RequiredLogicalProperty property, int childIndex) {
        Preconditions.checkState(false, "Scan does't have children.");
        return null;
    }
}
