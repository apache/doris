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

import com.google.common.collect.Lists;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptUtils;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptPhysicalSort;

import java.util.List;

public class OptOrderSpec implements OptPropertySpec {
    private List<OptOrderItem> orderItems = Lists.newArrayList();

    public OptOrderSpec() {
    }

    public static OptOrderSpec createEmpty() {
        return new OptOrderSpec();
    }

    public void addOrderItem(OptColumnRef columnRef, boolean isAsc, boolean isNullFirst) {
        orderItems.add(new OptOrderItem(columnRef, isAsc, isNullFirst));
    }

    public List<OptOrderItem> getOrderItems() { return orderItems; }
    public boolean isEmpty() { return orderItems.isEmpty(); }

    // check if this order specification satisfies given one
    public boolean contains(OptOrderSpec rhs) {
        if (orderItems.size() < rhs.orderItems.size()) {
            return false;
        }
        for (int i = 0; i < rhs.orderItems.size(); ++i) {
            if (!orderItems.get(i).matches(rhs.orderItems.get(i))) {
                return false;
            }
        }
        return true;
    }

    public OptColumnRefSet getUsedColumns() {
        OptColumnRefSet set = new OptColumnRefSet();
        for (OptOrderItem item : orderItems) {
            set.include(item.getColumnRef());
        }
        return set;
    }

    @Override
    public void appendEnforcers(
            RequiredPhysicalProperty reqdProp,
            OptExpression child,
            OptExpressionHandle exprHandle,
            List<OptExpression> expressions) {
        OptExpression expr = OptExpression.create(new OptPhysicalSort(this), child);
        expressions.add(expr);
    }

    @Override
    public int hashCode() {
        int code = 0;
        for (OptOrderItem item : orderItems) {
            code = OptUtils.combineHash(code, item.hashCode());
        }
        return code;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof OptOrderSpec)) {
            return false;
        }
        OptOrderSpec rhs = (OptOrderSpec) obj;
        if (orderItems.size() != rhs.orderItems.size()) {
            return false;
        }
        for (int i = 0; i < orderItems.size(); ++i) {
            if (!orderItems.get(i).equals(rhs.orderItems.get(i))) {
                return false;
            }
        }
        return true;
    }
}
