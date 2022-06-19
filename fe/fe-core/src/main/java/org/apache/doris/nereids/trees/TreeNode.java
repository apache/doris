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

package org.apache.doris.nereids.trees;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.Operator;

import java.util.List;
import java.util.Optional;

/**
 * interface for all node in Nereids, include plan node and expression.
 *
 * @param <NODE_TYPE> either {@link org.apache.doris.nereids.trees.plans.Plan}
 *                 or {@link org.apache.doris.nereids.trees.expressions.Expression}
 */
public interface TreeNode<NODE_TYPE extends TreeNode<NODE_TYPE>> {

    Operator getOperator();

    // cache GroupExpression for fast exit from Memo.copyIn.
    Optional<GroupExpression> getGroupExpression();

    NodeType getType();

    List<NODE_TYPE> children();

    NODE_TYPE child(int index);

    int arity();

    NODE_TYPE withChildren(List<NODE_TYPE> children);
}
