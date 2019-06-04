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
import org.apache.doris.optimizer.base.*;

import java.util.List;

public final class OptPhysicalHashAggregate extends OptPhysical {

    private static final List<OptColumnRef> EMPTY_GROUPBYS = Lists.newArrayList();
    private final List<OptColumnRef> groupBys;
    private final OptOperator.AggType type;

    private OptPhysicalHashAggregate(OptOperator.AggType type) {
        this(EMPTY_GROUPBYS, type);
    }

    private OptPhysicalHashAggregate(List<OptColumnRef> groupBys, OptOperator.AggType type) {
        super(OptOperatorType.OP_PHYSICAL_HASH_AGG);
        this.groupBys = groupBys;
        this.type = type;
    }

    public static OptPhysicalHashAggregate createLocalAggregate(List<OptColumnRef> groupBys) {
        return new OptPhysicalHashAggregate(AggType.GB_LOCAL);
    }

    public static OptPhysicalHashAggregate createIntermidiateAggregate(List<OptColumnRef> groupBys) {
        return new OptPhysicalHashAggregate(AggType.GB_INTERMEDIATE);
    }

    public static OptPhysicalHashAggregate createGlobalAggregate(List<OptColumnRef> groupBys) {
        return new OptPhysicalHashAggregate(AggType.GB_GLOBAL);
    }

    public List<OptColumnRef> getGroupBys() {
        return groupBys;
    }

    @Override
    public OptDistributionSpec getDistributionSpec(OptExpressionHandle exprHandle) {
        final OptDistributionSpec childDistribution =
                exprHandle.getChildPhysicalProperty(0).getDistributionSpec();
        return childDistribution;
    }

    @Override
    public OrderEnforcerProperty getChildReqdOrder(OptExpressionHandle handle,
                                                   OrderEnforcerProperty reqdOrder, int childIndex) {
        return OrderEnforcerProperty.EMPTY;
    }

    @Override
    public DistributionEnforcerProperty getChildReqdDistribution(
            OptExpressionHandle handle, DistributionEnforcerProperty reqdDistribution, int childIndex) {
        if (type == AggType.GB_INTERMEDIATE || (type == AggType.GB_GLOBAL && groupBys.size() != 0)) {
            final OptLogicalProperty logicalProperty = handle.getLogicalProperty();
            final OptColumnRefSet columns = new OptColumnRefSet();
            columns.include(groupBys);
            columns.intersects(logicalProperty.getOutputColumns());
            final OptHashDistributionItem item = new OptHashDistributionItem(columns);
            return new DistributionEnforcerProperty(OptDistributionSpec.createHashDistributionSpec(item));
        } else  {
            return DistributionEnforcerProperty.ANY;
        }
    }

    @Override
    protected OptColumnRefSet deriveChildReqdColumns(OptExpressionHandle exprHandle,
                                                  RequiredPhysicalProperty property, int childIndex) {
        Preconditions.checkArgument(childIndex == 1, "Aggregate only hava one ");

        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());
        columns.include(groupBys);

        // Having predicates
        final int childrenSize = exprHandle.getChildPropertySize();
        for (int itemIndex = 1; itemIndex < childrenSize; itemIndex++) {
            final OptItemProperty itemProperty = exprHandle.getChildItemProperty(itemIndex);
            columns.include(itemProperty.getUsedColumns());
            columns.intersects(itemProperty.getGeneratedColumns());
        }

        final OptLogicalProperty logicalProperty = exprHandle.getChildLogicalProperty(childIndex);
        columns.intersects(logicalProperty.getOutputColumns());
        return columns;
    }
}
