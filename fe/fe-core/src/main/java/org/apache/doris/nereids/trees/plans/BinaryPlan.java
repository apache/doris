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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.operators.plans.BinaryPlanOperator;
import org.apache.doris.nereids.trees.BinaryNode;

import java.util.List;

/**
 * interface for all plan that have two children.
 */
public interface BinaryPlan<
            PLAN_TYPE extends BinaryPlan<PLAN_TYPE, OP_TYPE, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>,
            OP_TYPE extends BinaryPlanOperator,
            LEFT_CHILD_TYPE extends Plan,
            RIGHT_CHILD_TYPE extends Plan>
        extends Plan<PLAN_TYPE, OP_TYPE>, BinaryNode<PLAN_TYPE, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    @Override
    List<Plan> children();

    @Override
    Plan child(int index);

    @Override
    default LEFT_CHILD_TYPE left() {
        return BinaryNode.super.left();
    }

    @Override
    default RIGHT_CHILD_TYPE right() {
        return BinaryNode.super.right();
    }
}
