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
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Expression for alias, such as col1 as c1.
 */
public class Alias<CHILD_TYPE extends Expression> extends NamedExpression
        implements UnaryExpression<CHILD_TYPE> {

    private final ExprId exprId;
    private final String name;
    private final List<String> qualifier;

    /**
     * constructor of Alias.
     *
     * @param child expression that alias represents for
     * @param name alias name
     */
    public Alias(CHILD_TYPE child, String name) {
        super(NodeType.ALIAS, child);
        this.exprId = NamedExpressionUtil.newExprId();
        this.name = name;
        this.qualifier = ImmutableList.of();
    }

    @Override
    public Slot toSlot() throws UnboundException {
        return new SlotReference(exprId, name, child().getDataType(), child().nullable(), qualifier);
    }

    @Override
    public String getName() throws UnboundException {
        return name;
    }

    @Override
    public ExprId getExprId() throws UnboundException {
        return exprId;
    }

    @Override
    public List<String> getQualifier() {
        return qualifier;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return child().getDataType();
    }

    @Override
    public String sql() {
        return null;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return child().nullable();
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Alias<>(children.get(0), name);
    }

    @Override
    public String toString() {
        return child().toString() + " AS " + name;
    }
}
