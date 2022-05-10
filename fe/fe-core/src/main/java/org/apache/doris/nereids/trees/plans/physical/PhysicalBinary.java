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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.plans.BinaryPlan;
import org.apache.doris.nereids.trees.plans.Plan;

/**
 * Abstract class for all physical plan that have two children.
 */
public abstract class PhysicalBinary<
            PLAN_TYPE extends PhysicalBinary<PLAN_TYPE, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>,
            LEFT_CHILD_TYPE extends Plan,
            RIGHT_CHILD_TYPE extends Plan>
        extends AbstractPhysicalPlan<PLAN_TYPE>
        implements BinaryPlan<PLAN_TYPE, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    public PhysicalBinary(NodeType type, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(type, leftChild, rightChild);
    }
}
