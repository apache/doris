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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ExprSubstitutionMap.java
// and modified by Doris

package org.apache.doris.analysis;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Map of expression substitutions: lhs[i] gets substituted with rhs[i].
 * To support expression substitution across query blocks, rhs exprs must already be
 * analyzed when added to this map. Otherwise, analysis of a SlotRef may fail after
 * substitution, e.g., because the table it refers to is in a different query block
 * that is not visible.
 * See Expr.substitute() and related functions for details on the actual substitution.
 */
public final class ExprSubstitutionMap {

    private List<Expr> lhs; // left-hand side
    private List<Expr> rhs; // right-hand side

    public ExprSubstitutionMap() {
        this(Lists.<Expr>newArrayList(), Lists.<Expr>newArrayList());
    }

    public ExprSubstitutionMap(List<Expr> lhs, List<Expr> rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    /**
     * Add an expr mapping. The rhsExpr must be analyzed to support correct substitution
     * across query blocks. It is not required that the lhsExpr is analyzed.
     */
    public void put(Expr lhsExpr, Expr rhsExpr) {
        lhs.add(lhsExpr);
        rhs.add(rhsExpr);
    }

    public List<Expr> getLhs() {
        return lhs;
    }

    public List<Expr> getRhs() {
        return rhs;
    }

    @Override
    public ExprSubstitutionMap clone() {
        return new ExprSubstitutionMap(Expr.cloneList(lhs), Expr.cloneList(rhs));
    }
}
