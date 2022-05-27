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

package org.apache.doris.nereids.operators;

import org.apache.doris.nereids.PlanOperatorVisitor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import java.util.Objects;

/**
 * Abstract class for all concrete operator.
 */
public abstract class AbstractOperator<TYPE extends AbstractOperator<TYPE>> implements Operator<TYPE> {
    protected final OperatorType type;

    public AbstractOperator(OperatorType type) {
        this.type = Objects.requireNonNull(type, "type can not be null");
    }

    @Override
    public OperatorType getType() {
        return type;
    }

    public <R, C> R accept(PlanOperatorVisitor<R, C> visitor, PhysicalPlan<?,?>  physicalPlan, C context){ return null; }
}
