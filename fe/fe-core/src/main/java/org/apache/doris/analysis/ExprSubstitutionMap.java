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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final static Logger LOG = LoggerFactory.getLogger(ExprSubstitutionMap.class);

    private boolean checkAnalyzed = true;
    private List<Expr> lhs; // left-hand side
    private List<Expr> rhs; // right-hand side

    public ExprSubstitutionMap() {
        this(Lists.<Expr>newArrayList(), Lists.<Expr>newArrayList());
    }

    // Only used to convert show statement to select statement
    public ExprSubstitutionMap(boolean checkAnalyzed) {
        this(Lists.<Expr>newArrayList(), Lists.<Expr>newArrayList());
        this.checkAnalyzed = checkAnalyzed;
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
        Preconditions.checkState(!checkAnalyzed || rhsExpr.isAnalyzed(),
                "Rhs expr must be analyzed.");
        lhs.add(lhsExpr);
        rhs.add(rhsExpr);
    }

    /**
     * Returns the expr mapped to lhsExpr or null if no mapping to lhsExpr exists.
     */
    public Expr get(Expr lhsExpr) {
        for (int i = 0; i < lhs.size(); ++i) {
            if (lhsExpr.equals(lhs.get(i))) {
                return rhs.get(i);
            }
        }
        return null;
    }

    /**
     * Returns true if the smap contains a mapping for lhsExpr.
     */
    public boolean containsMappingFor(Expr lhsExpr) {
        return lhs.contains(lhsExpr);
    }

    /**
     * Return a map  which is equivalent to applying f followed by g,
     * i.e., g(f()).
     * Always returns a non-null map.
     */
    public static ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
                                              Analyzer analyzer) {
        if (f == null && g == null) {
            return new ExprSubstitutionMap();
        }
        if (f == null) {
            return g;
        }
        if (g == null) {
            return f;
        }
        ExprSubstitutionMap result = new ExprSubstitutionMap();
        // f's substitution targets need to be substituted via g
        result.lhs = Expr.cloneList(f.lhs);
        result.rhs = Expr.substituteList(f.rhs, g, analyzer, false);

        // substitution maps are cumulative: the combined map contains all
        // substitutions from f and g.
        for (int i = 0; i < g.lhs.size(); i++) {
            // If f contains expr1->fn(expr2) and g contains expr2->expr3,
            // then result must contain expr1->fn(expr3).
            // The check before adding to result.lhs is to ensure that cases
            // where expr2.equals(expr1) are handled correctly.
            // For example f: count(*) -> zeroifnull(count(*))
            // and g: count(*) -> slotref
            // result.lhs must only have: count(*) -> zeroifnull(slotref) from f above,
            // and not count(*) -> slotref from g as well.
            if (!result.lhs.contains(g.lhs.get(i))) {
                result.lhs.add(g.lhs.get(i).clone());
                result.rhs.add(g.rhs.get(i).clone());
            }
        }

        result.verify();
        return result;
    }

    /**
     * Returns the union of two substitution maps. Always returns a non-null map.
     */
    public static ExprSubstitutionMap combine(ExprSubstitutionMap f,
                                              ExprSubstitutionMap g) {
        if (f == null && g == null) {
            return new ExprSubstitutionMap();
        }
        if (f == null) {
            return g;
        }
        if (g == null) {
            return f;
        }
        ExprSubstitutionMap result = new ExprSubstitutionMap();
        result.lhs = Lists.newArrayList(f.lhs);
        result.lhs.addAll(g.lhs);
        result.rhs = Lists.newArrayList(f.rhs);
        result.rhs.addAll(g.rhs);
        result.verify();
        return result;
    }

    public void substituteLhs(ExprSubstitutionMap lhsSmap, Analyzer analyzer) {
        lhs = Expr.substituteList(lhs, lhsSmap, analyzer, false);
    }

    public List<Expr> getLhs() { return lhs; }
    public List<Expr> getRhs() { return rhs; }

    public int size() { return lhs.size(); }

    public String debugString() {
        Preconditions.checkState(lhs.size() == rhs.size());
        List<String> output = Lists.newArrayList();
        for (int i = 0; i < lhs.size(); ++i) {
            output.add(lhs.get(i).toSql() + ":" + rhs.get(i).toSql());
            output.add("(" + lhs.get(i).debugString() + ":" + rhs.get(i).debugString() + ")");
        }
        return "smap(" + Joiner.on(" ").join(output) + ")";
    }

    /**
     * Verifies the internal state of this smap: Checks that the lhs_ has no duplicates,
     * and that all rhs exprs are analyzed.
     */
    private void verify() {
        for (int i = 0; i < lhs.size(); ++i) {
            for (int j = i + 1; j < lhs.size(); ++j) {
                if (lhs.get(i).equals(lhs.get(j))) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("verify: smap=" + this.debugString());
                    }
                    // TODO(zc): partition by k1, order by k1, there is failed.
                    // Preconditions.checkState(false);
                }
            }
            Preconditions.checkState(!checkAnalyzed || rhs.get(i).isAnalyzed());
        }
    }

    public void clear() {
        lhs.clear();
        rhs.clear();
    }

    @Override
    public ExprSubstitutionMap clone() {
        return new ExprSubstitutionMap(Expr.cloneList(lhs), Expr.cloneList(rhs));
    }
}
