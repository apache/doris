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

package org.apache.doris.optimizer.base;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.OptUtils;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptPhysical;

import java.util.List;

public class RequiredPhysicalProperty extends RequiredProperty {
    private OrderEnforcerProperty orderProperty;
    private DistributionEnforcerProperty distributionProperty;

    public RequiredPhysicalProperty() {
    }

    public static RequiredPhysicalProperty createTestProperty() {
        final RequiredPhysicalProperty property = new RequiredPhysicalProperty();
        property.orderProperty = OrderEnforcerProperty.EMPTY;
        property.distributionProperty = DistributionEnforcerProperty.ANY;
        property.columns = new OptColumnRefSet();
        return property;
    }

    @Override
    public void compute(OptExpressionHandle exprHandle, RequiredProperty parentProperty, int childIndex) {
        Preconditions.checkArgument(parentProperty instanceof RequiredPhysicalProperty,
                "parentProperty can only be physical property.");
        final RequiredPhysicalProperty parentPhysicalProperty =
                (RequiredPhysicalProperty) parentProperty;
        final OptPhysical physical = (OptPhysical) exprHandle.getOp();
        columns = physical.getChildReqdColumns(exprHandle, parentPhysicalProperty, childIndex);
        orderProperty = physical.getChildReqdOrder(exprHandle,
                parentPhysicalProperty.getOrderProperty(),
                        childIndex);
        distributionProperty = physical.getChildReqdDistribution(
                        exprHandle,
                parentPhysicalProperty.getDistributionProperty(),
                        childIndex);
    }

    public OrderEnforcerProperty getOrderProperty() { return orderProperty; }
    public DistributionEnforcerProperty getDistributionProperty() { return distributionProperty; }

    public EnforcerProperty.EnforceType getEnforcerOrderType(
            OptPhysical operator, OptExpressionHandle exprHandle) {
        Preconditions.checkNotNull(orderProperty);
        return operator.getOrderEnforceType(exprHandle, orderProperty);
    }

    public EnforcerProperty.EnforceType getEnforcerDistributionType(
            OptPhysical operator, OptExpressionHandle exprHandle) {
        Preconditions.checkNotNull(distributionProperty);
        return operator.getDistributionEnforcerType(exprHandle, distributionProperty);
    }

    @Override
    public int hashCode() {
        int code = columns.hashCode();
        code = OptUtils.combineHash(code, orderProperty.hashCode());
        return OptUtils.combineHash(code, distributionProperty.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof RequiredPhysicalProperty)) {
            return false;
        }

        if (this == obj) {
            return true;
        }

        final RequiredPhysicalProperty rhs = (RequiredPhysicalProperty) obj;
        return columns.equals(rhs.columns)
                && orderProperty.equals(rhs.orderProperty)
                && distributionProperty.equals(rhs.distributionProperty);
    }
}
