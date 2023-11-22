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
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Optional;

/**
 * Expression in Nereids that having name.
 */
public abstract class NamedExpression extends Expression {

    protected NamedExpression(List<Expression> children) {
        super(children);
    }

    public Slot toSlot() throws UnboundException {
        throw new UnboundException("toSlot");
    }

    public String getName() throws UnboundException {
        throw new UnboundException("name");
    }

    public ExprId getExprId() throws UnboundException {
        throw new UnboundException("exprId");
    }

    public List<String> getQualifier() throws UnboundException {
        throw new UnboundException("qualifier");
    }

    /**
     * Get qualified name of NamedExpression.
     *
     * @return qualified name
     * @throws UnboundException throw this exception if this expression is unbound
     */
    public String getQualifiedName() throws UnboundException {
        return Utils.qualifiedName(getQualifier(), getName());
    }

    @Override
    public String getExpressionName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(getName(), DEFAULT_EXPRESSION_NAME));
        }
        return this.exprName.get();
    }
}
