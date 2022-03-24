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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.common.AnalysisException;

/**
 * Base class for all Expr equal rules. A rule is free to modify Exprs in place,
 * but must return a different Expr object if any modifications were made.
 * An ExprRewriteRule is intended to apply its transformation on a single Expr and not
 * recursively on all its children. The recursive and repeated application of
 * ExprRewriteRules is driven by an ExprRewriter.
 * An ExprRewriteRule should preserve the type of the transformed expression to avoid
 * having to re-analyze its parent (if any). This behavior guarantees that the Expr
 * tree remains valid even if arbitrary subexpressions are transformed. It also means
 * that the transformed expression may have a wider type than necessary. Callers are
 * free to reset() and analyze() the result if they desire the minimal type.
 */
public interface ExprRewriteRule {
    /**
     * Applies this equal rule to the given analyzed Expr. Returns the transformed and
     * analyzed Expr or the original unmodified Expr if no changes were made. If any
     * changes were made, the transformed Expr is guaranteed to be a different Expr object,
     * so callers can rely on object reference comparison for change detection.
     */
    Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException;
}
