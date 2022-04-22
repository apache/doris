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

import com.clearspring.analytics.util.Lists;

import java.util.List;

/**
 * Expression for alias, such as col1 as c1.
 */
public class Alias extends NamedExpression {
    private final ExprId exprId;
    private final String name;
    private final List<String> qualifier;

    /**
     * constructor of Alias.
     *
     * @param child expression that alias represents for
     * @param name alias name
     */
    public Alias(Expression child, String name) {
        super(NodeType.ALIAS);
        exprId = NamedExpression.newExprId();
        this.name = name;
        qualifier = Lists.newArrayList();
        addChild(child);
    }

    public Expression child() {
        return getChild(0);
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
    public Slot toAttribute() throws UnboundException {
        return new SlotReference(exprId, name, child().getDataType(), child().nullable(), qualifier);
    }

    @Override
    public String sql() {
        return null;
    }

    @Override
    public String toString() {
        return child().toString() + " AS " + name;
    }
}
