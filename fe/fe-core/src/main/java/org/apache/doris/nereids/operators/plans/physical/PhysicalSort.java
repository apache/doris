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

package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import java.util.List;

public class PhysicalSort extends PhysicalUnaryOperator<PhysicalSort, PhysicalPlan> {

    private int offset;

    private int limit = -1;

    private List<OrderKey> orderList;

    private boolean useTopN;

    public PhysicalSort(int offset, int limit, List<OrderKey> orderList, boolean useTopN) {
        super(OperatorType.PHYSICAL_SORT);
        this.offset = offset;
        this.limit = limit;
        this.orderList = orderList;
        this.useTopN = useTopN;
    }

    public PhysicalSort() {
        super(OperatorType.PHYSICAL_SORT);
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    public List<OrderKey> getOrderList() {
        return orderList;
    }

    public boolean isUseTopN() {
        return useTopN;
    }

    public boolean hasLimit() {
        return limit > -1;
    }
}
