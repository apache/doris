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

import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Expression in Nereids that having name.
 */
public interface NamedExpression<EXPR_TYPE extends NamedExpression<EXPR_TYPE>>
        extends Expression<EXPR_TYPE> {

    @Override
    Expression child(int index);

    @Override
    List<Expression> children();

    default Slot toAttribute() throws UnboundException {
        throw new UnboundException("toAttribute");
    }

    default String getName() throws UnboundException {
        throw new UnboundException("name");
    }

    default ExprId getExprId() throws UnboundException {
        throw new UnboundException("exprId");
    }

    default List<String> getQualifier() throws UnboundException {
        throw new UnboundException("qualifier");
    }

    /**
     * Get qualified name of NamedExpression.
     *
     * @return qualified name
     * @throws UnboundException throw this exception if this expression is unbound
     */
    default String getQualifiedName() throws UnboundException {
        String qualifiedName = "";
        if (CollectionUtils.isNotEmpty(getQualifier())) {
            qualifiedName = String.join(".", getQualifier()) + ".";
        }
        return qualifiedName + getName();
    }

    /**
     * Tool class for generate next ExprId.
     */
    class NamedExpressionUtils {
        static final UUID JVM_ID = UUID.randomUUID();
        private static final AtomicLong CURRENT_ID = new AtomicLong();

        static ExprId newExprId() {
            return new ExprId(CURRENT_ID.getAndIncrement(), JVM_ID);
        }
    }
}
