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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.BinaryNode;
import org.apache.doris.nereids.trees.NodeType;

/**
 * Abstract class for all expression that have two children.
 */
public abstract class BinaryExpression<
            EXPR_TYPE extends BinaryExpression<EXPR_TYPE, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>,
            LEFT_CHILD_TYPE extends Expression,
            RIGHT_CHILD_TYPE extends Expression>
        extends AbstractExpression<EXPR_TYPE>
        implements BinaryNode<EXPR_TYPE, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    public BinaryExpression(NodeType type, LEFT_CHILD_TYPE left, RIGHT_CHILD_TYPE right) {
        super(type, left, right);
    }

    @Override
    public LEFT_CHILD_TYPE left() {
        return BinaryNode.super.left();
    }

    @Override
    public RIGHT_CHILD_TYPE right() {
        return BinaryNode.super.right();
    }
}
