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
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.*;

import java.util.List;

public class OptPhysicalHashJoin extends OptPhysical {

    public OptPhysicalHashJoin() {
        super(OptOperatorType.OP_PHYSICAL_HASH_JOIN);
    }

    @Override
    public OrderEnforcerProperty getChildReqdOrder(
            OptExpressionHandle handle, OrderEnforcerProperty reqdOrder, int childIndex) {
        if (childIndex == 0) {
            return reqdOrder;
        }
        return OrderEnforcerProperty.EMPTY;
    }

    @Override
    public DistributionEnforcerProperty getChildReqdDistribution(
            OptExpressionHandle exprHandle, DistributionEnforcerProperty reqdDistribution, int childIndex) {
        // TODO ch, cache it.
        final OptLogicalProperty childProperty = exprHandle.getChildLogicalProperty(childIndex);
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.intersects(childProperty.getOutputColumns());
        Preconditions.checkArgument(columns.cardinality() > 0);

        final OptHashDistributionItem item = new OptHashDistributionItem(columns);
        return new DistributionEnforcerProperty(OptDistributionSpec.createHashDistributionSpec(item));
    }

    @Override
    protected OptColumnRefSet deriveChildReqdColumns(
            OptExpressionHandle exprHandle, RequiredPhysicalProperty property, int childIndex) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());

        // Join's predicates
        for (int conjunctIndex = 2; exprHandle.getChildItemProperty(conjunctIndex) != null; conjunctIndex++) {
            final OptItemProperty itemProperty = exprHandle.getChildItemProperty(conjunctIndex);
            columns.include(itemProperty.getUsedColumns());
        }

        // Join's outer or inner child.
        final OptLogicalProperty childProperty = exprHandle.getChildLogicalProperty(childIndex);
        columns.intersects(childProperty.getOutputColumns());

        return columns;
    }
}
