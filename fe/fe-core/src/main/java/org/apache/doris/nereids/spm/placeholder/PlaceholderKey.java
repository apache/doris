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

package org.apache.doris.nereids.spm.placeholder;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;

import java.util.List;
import java.util.Map;

/** Query-block and expression-occurrence identity used to reuse placeholder ids. */
record PlaceholderKey(Expression expression, Expression parent, int childIndex,
        long blockId, int projectItem, long rootOrdinal, String occurrencePath,
        Map<Expression, String> parentSignatures) {
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PlaceholderKey)) {
            return false;
        }
        PlaceholderKey key = (PlaceholderKey) other;
        if (blockId != key.blockId || projectItem != key.projectItem || rootOrdinal != key.rootOrdinal
                || childIndex != key.childIndex || !occurrencePath.equals(key.occurrencePath)
                || !expression.equals(key.expression)) {
            return false;
        }
        if (parent == key.parent) {
            return true;
        }
        return parent != null && key.parent != null
                && signature().equals(key.signature());
    }

    private String signature() {
        return parentSignatures.computeIfAbsent(parent, PlaceholderKey::parentSignature);
    }

    @Override
    public int hashCode() {
        int hash = expression.hashCode();
        hash = 31 * hash + childIndex;
        hash = 31 * hash + Long.hashCode(blockId);
        hash = 31 * hash + projectItem;
        hash = 31 * hash + Long.hashCode(rootOrdinal);
        hash = 31 * hash + occurrencePath.hashCode();
        // Parent shape participates in equality but not hashing. Different occurrences
        // already separate independent positions without constructing signature strings.
        return 31 * hash + (parent == null ? 0 : 1);
    }

    static PlaceholderKey of(Expression expression, Expression parent, int childIndex,
            long blockId, int projectItem, long rootOrdinal, String occurrencePath,
            Map<Expression, String> parentSignatures) {
        return new PlaceholderKey(expression, parent, parent == null ? 0 : childIndex,
                blockId, projectItem, rootOrdinal, occurrencePath == null ? "" : occurrencePath,
                parentSignatures);
    }

    private static String parentSignature(Expression expr) {
        if (expr instanceof SubqueryExpr) {
            return "<subquery>";
        }
        List<Expression> children = expr.children();
        if (children.isEmpty()) {
            return expr.getClass().getSimpleName() + ":" + expr.toSql();
        }
        StringBuilder sb = new StringBuilder(expr.getClass().getSimpleName());
        for (Expression child : children) {
            sb.append('(').append(parentSignature(child)).append(')');
        }
        return sb.toString();
    }

}
