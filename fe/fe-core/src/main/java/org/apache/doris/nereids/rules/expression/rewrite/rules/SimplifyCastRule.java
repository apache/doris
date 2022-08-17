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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;


/**
 * Rewrite rule of simplify CAST expression.
 * Remove redundant cast like
 * - cast(1 as int) -> 1.
 * Merge cast like
 * - cast(cast(1 as bigint) as string) -> cast(1 as string).
 */
public class SimplifyCastRule extends AbstractExpressionRewriteRule {

    public static SimplifyCastRule INSTANCE = new SimplifyCastRule();

    @Override
    public Expression visitCast(Cast origin, ExpressionRewriteContext context) {
        return simplify(origin);
    }

    private Expression simplify(Cast cast) {
        Expression source = cast.left();
        // simplify inside
        if (source instanceof Cast) {
            source = simplify((Cast) source);
        }

        // remove redundant cast
        // CAST(value as type), value is type
        if (cast.getDataType().equals(source.getDataType())) {
            return source;
        }

        if (source != cast.left()) {
            return new Cast(source, cast.right());
        }
        return cast;
    }
}
