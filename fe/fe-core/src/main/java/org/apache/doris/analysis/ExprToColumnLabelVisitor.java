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

package org.apache.doris.analysis;

/**
 * Visitor that produces the column-label string for any {@link Expr}.
 *
 * <p>For all expression types except {@link SlotRef}, the result is identical
 * to {@link ExprToSqlVisitor} (i.e. the standard SQL string).  For
 * {@link SlotRef} the result is the raw column name without table qualification
 * or backtick quoting, matching the legacy {@code SlotRef.toColumnLabel()}
 * behaviour.
 *
 * <p>Usage:
 * <pre>
 *     String label = expr.accept(ExprToColumnLabelVisitor.INSTANCE, null);
 * </pre>
 */
public class ExprToColumnLabelVisitor extends ExprToSqlVisitor {

    /** Singleton — the visitor is stateless. */
    public static final ExprToColumnLabelVisitor INSTANCE = new ExprToColumnLabelVisitor();

    private ExprToColumnLabelVisitor() {
    }

    /**
     * For a {@link SlotRef} the column label is the raw column name only
     * (no table prefix, no backtick quoting), matching the legacy
     * {@code SlotRef.toColumnLabel()} override that simply returned {@code col}.
     */
    @Override
    public String visitSlotRef(SlotRef expr, ToSqlParams context) {
        return expr.getCol();
    }
}
