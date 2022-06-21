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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Abstract class for all Expression in Nereids.
 */
public abstract class Expression extends AbstractTreeNode<Expression> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public Expression(NodeType type, Expression... children) {
        super(type, children);
    }

    public DataType getDataType() throws UnboundException {
        throw new UnboundException("dataType");
    }

    public String sql() throws UnboundException {
        throw new UnboundException("sql");
    }

    public boolean nullable() throws UnboundException {
        throw new UnboundException("nullable");
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        logger.warn("accept() is not implemented by " + this.getClass());
        return visitor.visit(this, context);
    }

    @Override
    public List<Expression> children() {
        return (List) children;
    }

    @Override
    public Expression child(int index) {
        return (Expression) children.get(index);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        throw new RuntimeException();
    }

    /**
     * Whether the expression is a constant.
     */
    public boolean isConstant() {
        for (Expression child : children()) {
            if (child.isConstant()) {
                return true;
            }
        }
        return false;
    }
}
