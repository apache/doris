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

public class OptPhysicalDistribution extends OptPhysical {

    public OptPhysicalDistribution(OptDistributionSpec spec) {
        super(OptOperatorType.OP_PHYSICAL_DISTRIBUTION, spec);
    }

    @Override
    public OrderEnforcerProperty getChildReqdOrder(OptExpressionHandle handle,
                                                   OrderEnforcerProperty reqdOrder, int childIndex) {
        // Distribution does't impact the children's order.
        return reqdOrder;
    }

    @Override
    public DistributionEnforcerProperty getChildReqdDistribution(
            OptExpressionHandle handle, DistributionEnforcerProperty reqdDistribution, int childIndex) {
        return DistributionEnforcerProperty.ANY;
    }

    @Override
    public EnforcerProperty.EnforceType getDistributionEnforcerType(
            OptExpressionHandle exprHandle, DistributionEnforcerProperty property) {
        if (distributionSpec.isSatisfy(property.getPropertySpec())) {
            return EnforcerProperty.EnforceType.UNNECESSARY;
        }
        return EnforcerProperty.EnforceType.PROHIBITED;
    }

    @Override
    protected OptColumnRefSet deriveChildReqdColumns(OptExpressionHandle exprHandle,
                                                  RequiredPhysicalProperty property, int childIndex) {
        Preconditions.checkArgument(exprHandle.getChildPropertySize() == 1 && childIndex == 0,
                " Distribution can only have one child.");
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());
        final OptHashDistributionItem item = distributionSpec.getHashItem();
        columns.include(item.getColumns());
        final OptLogicalProperty logicalProperty = exprHandle.getChildLogicalProperty(childIndex);
        columns.intersects(logicalProperty.getOutputColumns());
        return columns;
    }

}
