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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.operators.plans.logical.LogicalUnaryOperator;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;

/**
 * Abstract class for all logical plan that have one child.
 */
public class LogicalUnary<OP_TYPE extends LogicalUnaryOperator, CHILD_TYPE extends Plan>
        extends AbstractLogicalPlan<LogicalUnary<OP_TYPE, CHILD_TYPE>, OP_TYPE>
        implements UnaryPlan<LogicalUnary<OP_TYPE, CHILD_TYPE>, OP_TYPE, CHILD_TYPE> {

    public LogicalUnary(OP_TYPE operator, CHILD_TYPE child) {
        super(NodeType.LOGICAL, operator, child);
    }
}
