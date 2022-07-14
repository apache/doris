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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.ExpressionType;
import org.apache.doris.nereids.trees.expressions.LeafExpression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.Utils;

import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Star expression.
 */
public class UnboundStar extends NamedExpression implements LeafExpression, Unbound {
    private final List<String> qualifier;

    public UnboundStar(List<String> qualifier) {
        super(ExpressionType.UNBOUND_STAR);
        this.qualifier = qualifier;
    }

    @Override
    public String toSql() {
        String qualified = qualifier.stream().map(Utils::quoteIfNeeded).reduce((t1, t2) -> t1 + "." + t2).orElse("");
        if (StringUtils.isNotEmpty(qualified)) {
            return qualified + ".*";
        } else {
            return "*";
        }
    }

    @Override
    public List<String> getQualifier() throws UnboundException {
        return qualifier;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundStar(this, context);
    }
}
