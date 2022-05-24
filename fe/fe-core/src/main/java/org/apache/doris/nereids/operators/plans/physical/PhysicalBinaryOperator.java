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

import org.apache.doris.nereids.operators.AbstractOperator;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.BinaryPlanOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;

/**
 * Abstract class for all physical operator that have two inputs.
 */
public abstract class PhysicalBinaryOperator<
            TYPE extends PhysicalBinaryOperator<TYPE, LEFT_INPUT_TYPE, RIGHT_INPUT_TYPE>,
            LEFT_INPUT_TYPE extends Plan,
            RIGHT_INPUT_TYPE extends Plan>
        extends AbstractOperator<TYPE>
        implements PhysicalOperator<TYPE>, BinaryPlanOperator<TYPE, LEFT_INPUT_TYPE, RIGHT_INPUT_TYPE> {

    public PhysicalBinaryOperator(OperatorType type) {
        super(type);
    }

    @Override
    public final List<Slot> computeOutputs(LogicalProperties logicalProperties, Plan... inputs) {
        return doComputeOutput(logicalProperties, (LEFT_INPUT_TYPE) inputs[0], (RIGHT_INPUT_TYPE) inputs[1]);
    }

    public List<Slot> doComputeOutput(LogicalProperties logicalProperties,
            LEFT_INPUT_TYPE left, RIGHT_INPUT_TYPE right) {
        return logicalProperties.getOutput();
    }
}
