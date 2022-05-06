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

import org.apache.doris.nereids.trees.NodeType;

/**
 * Abstract class for all {@link LogicalPlan} that have two children.
 */
public abstract class LogicalBinary extends LogicalPlan {

    /**
     * Constructor for LogicalBinary.
     *
     * @param type type for this plan.
     * @param left left child for LogicalBinary
     * @param right right child for LogicalBinary
     */
    public LogicalBinary(NodeType type, LogicalPlan left, LogicalPlan right) {
        super(type);
        addChild(left);
        addChild(right);
    }

    public LogicalPlan left() {
        return getChild(0);
    }

    public LogicalPlan right() {
        return getChild(1);
    }

    @Override
    public int arity() {
        return 2;
    }
}
