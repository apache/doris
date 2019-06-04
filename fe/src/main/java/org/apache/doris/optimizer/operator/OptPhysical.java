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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.*;

import java.util.List;
import java.util.Map;

public abstract class OptPhysical extends OptOperator {
    protected List<Map<RequiredPhysicalProperty, OptColumnRefSet>> columnsRequiredForChildrenCache;
    protected OptDistributionSpec distributionSpec;
    protected OptOrderSpec orderSpec;

    protected OptPhysical(OptOperatorType type) {
        this(type, OptDistributionSpec.createAnyDistributionSpec(), OptOrderSpec.createEmpty());
    }

    protected OptPhysical(OptOperatorType type, OptDistributionSpec distributionSpec) {
        this(type, distributionSpec, OptOrderSpec.createEmpty());
    }

    protected OptPhysical(OptOperatorType type, OptOrderSpec orderSpec) {
        this(type, OptDistributionSpec.createAnyDistributionSpec(), orderSpec);
    }

    protected OptPhysical(OptOperatorType type, OptDistributionSpec distributionSpec,
                          OptOrderSpec orderSpec) {
        super(type);
        this.distributionSpec = distributionSpec;
        this.orderSpec = orderSpec;
        this.columnsRequiredForChildrenCache = Lists.newArrayList();
    }

    @Override
    public boolean isPhysical() { return true; }
    @Override
    public OptProperty createProperty() {
        return new OptPhysicalProperty();
    }

    /*-------------------------------------------------------------------------------------------*/
    // These are called by RequiredPhysicalProperty for computing properties when optimizing children.

    public abstract OrderEnforcerProperty getChildReqdOrder(
            OptExpressionHandle handle, OrderEnforcerProperty reqdOrder, int childIndex);
    public abstract DistributionEnforcerProperty getChildReqdDistribution(
            OptExpressionHandle handle, DistributionEnforcerProperty reqdDistribution, int childIndex);
    protected abstract OptColumnRefSet deriveChildReqdColumns(OptExpressionHandle exprHandle,
                                                           RequiredPhysicalProperty property, int childIndex);

    public OptColumnRefSet getChildReqdColumns(OptExpressionHandle exprHandle,
                                               RequiredPhysicalProperty property, int childIndex) {
        Map<RequiredPhysicalProperty, OptColumnRefSet> requiredColumnsMap = null;
        if (childIndex < columnsRequiredForChildrenCache.size()) {
            requiredColumnsMap =
                    columnsRequiredForChildrenCache.get(childIndex);
            if (requiredColumnsMap != null && requiredColumnsMap.get(property) != null) {
                return requiredColumnsMap.get(property);
            }
        }

        final OptColumnRefSet columns = deriveChildReqdColumns(exprHandle, property, childIndex);
        if(requiredColumnsMap == null) {
            requiredColumnsMap = Maps.newHashMap();
            requiredColumnsMap.put(property, columns);
        }
        return columns;
    }
    /*-------------------------------------------------------------------------------------------*/

    public EnforcerProperty.EnforceType getOrderEnforceType(
            OptExpressionHandle exprHandle,
            OrderEnforcerProperty orderProperty) {
        if (orderSpec.isSatisfy(orderProperty.getPropertySpec())) {
            // required order is already established by sort operator
            return EnforcerProperty.EnforceType.UNNECESSARY;
        }
        return EnforcerProperty.EnforceType.REQUIRED;
    }

    public EnforcerProperty.EnforceType getDistributionEnforcerType(
            OptExpressionHandle exprHandle, DistributionEnforcerProperty property) {
        if (distributionSpec.isSatisfy(property.getPropertySpec())) {
            return EnforcerProperty.EnforceType.UNNECESSARY;
        }
        return EnforcerProperty.EnforceType.REQUIRED;
    }

    public OptOrderSpec getOrderSpec(OptExpressionHandle exprHandle) { return orderSpec; }
    public OptDistributionSpec getDistributionSpec(OptExpressionHandle exprHandle) { return distributionSpec; }

    protected final OptOrderSpec getOrderSpecPassThrough(OptExpressionHandle exprHandle) {
        return exprHandle.getChildPhysicalProperty(0).getOrderSpec();
    }
}
