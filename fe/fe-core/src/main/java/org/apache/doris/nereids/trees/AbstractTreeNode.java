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

import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.MutableState.EmptyMutableState;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for plan node in Nereids, include plan node and expression.
 *
 * @param <NODE_TYPE> either {@link org.apache.doris.nereids.trees.plans.Plan}
 *                 or {@link org.apache.doris.nereids.trees.expressions.Expression}
 */
public abstract class AbstractTreeNode<NODE_TYPE extends TreeNode<NODE_TYPE>>
        implements TreeNode<NODE_TYPE> {
    protected final List<NODE_TYPE> children;

    // this field is special, because other fields in tree node is immutable, but in some scenes, mutable
    // state is necessary. e.g. the rewrite framework need distinguish whether the plan is created by
    // rules, the framework can set this field to a state variable to quickly judge without new big plan.
    // we should avoid using it as much as possible, because mutable state is easy to cause bugs and
    // difficult to locate.
    private MutableState mutableState = EmptyMutableState.INSTANCE;

    protected AbstractTreeNode(NODE_TYPE... children) {
        // NOTE: ImmutableList.copyOf has additional clone of the list, so here we
        //       direct generate a ImmutableList
        this.children = Utils.fastToImmutableList(children);
    }

    protected AbstractTreeNode(List<NODE_TYPE> children) {
        // NOTE: ImmutableList.copyOf has additional clone of the list, so here we
        //       direct generate a ImmutableList
        this.children = Utils.fastToImmutableList(children);
    }

    @Override
    public NODE_TYPE child(int index) {
        return children.get(index);
    }

    @Override
    public List<NODE_TYPE> children() {
        return children;
    }

    @Override
    public <T> Optional<T> getMutableState(String key) {
        return mutableState.get(key);
    }

    @Override
    public void setMutableState(String key, Object state) {
        this.mutableState = this.mutableState.set(key, state);
    }

    public int arity() {
        return children.size();
    }
}
