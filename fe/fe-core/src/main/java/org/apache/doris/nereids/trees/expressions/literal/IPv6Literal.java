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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.IPv6Type;

/**
 * Represents IPv6 literal
 */
public class IPv6Literal extends Literal {

    private final String value;

    public IPv6Literal(String ipv6) {
        super(IPv6Type.INSTANCE);
        this.value = ipv6;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIPv6Literal(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            return new org.apache.doris.analysis.IPv6Literal(value);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }
}
