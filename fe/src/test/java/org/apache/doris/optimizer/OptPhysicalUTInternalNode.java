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

package org.apache.doris.optimizer;

import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptOperatorType;
import org.apache.doris.optimizer.operator.OptPhysical;

public class OptPhysicalUTInternalNode extends OptPhysical {

    private int value;

    public OptPhysicalUTInternalNode() {
        super(OptOperatorType.OP_PHYSICAL_UNIT_TEST_INTERNAL);
        this.value = OptUtils.getUTOperatorId();;
    }

    public int getValue() { return value; }

    @Override
    public int hashCode() {
        return OptUtils.combineHash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        final OptPhysicalUTInternalNode rhs = (OptPhysicalUTInternalNode) object;
        return value == rhs.value;
    }

    @Override
    public String getExplainString(String prefix) { return type.getName() + " (value=" + value + ")"; }

    @Override
    public OrderEnforcerProperty getChildReqdOrder(
            OptExpressionHandle handle, OrderEnforcerProperty reqdOrder, int childIndex) {
        return OrderEnforcerProperty.EMPTY;
    }

    @Override
    public DistributionEnforcerProperty getChildReqdDistribution(
            OptExpressionHandle handle, DistributionEnforcerProperty reqdDistribution, int childIndex) {
        return DistributionEnforcerProperty.ANY;
    }

    @Override
    public OptColumnRefSet deriveChildReqdColumns(
            OptExpressionHandle exprHandle, RequiredPhysicalProperty property, int childIndex) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());
        final OptLogicalProperty childLogicalProperty = exprHandle.getChildLogicalProperty(childIndex);
        columns.intersects(childLogicalProperty.getOutputColumns());
        return columns;
    }
}
