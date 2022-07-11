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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.plans.physical.PhysicalHeapSort;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Spec of sort order.
 */
public class OrderSpec {
    private final List<OrderKey> orderKeys;

    public OrderSpec(List<OrderKey> orderKeys) {
        this.orderKeys = orderKeys;
    }

    /**
     * Whether other `OrderSpec` is satisfied the current `OrderSpec`.
     *
     * @param other another OrderSpec.
     */
    public boolean meet(OrderSpec other) {
        if (this.orderKeys.size() < other.getOrderKeys().size()) {
            return false;
        }
        for (int i = 0; i < other.getOrderKeys().size(); ++i) {
            if (!this.orderKeys.get(i).matches(other.getOrderKeys().get(i))) {
                return false;
            }
        }
        return true;
    }

    public GroupExpression addEnforcer(Group child) {
        return new GroupExpression(
                new PhysicalHeapSort(orderKeys, -1, 0),
                Lists.newArrayList(child)
        );
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }
}
