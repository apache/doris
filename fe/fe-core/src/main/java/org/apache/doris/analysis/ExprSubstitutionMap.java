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

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * Map of expression substitutions: lhs[i] gets substituted with rhs[i].
 * To support expression substitution across query blocks, rhs exprs must already be
 * analyzed when added to this map. Otherwise, analysis of a SlotRef may fail after
 * substitution, e.g., because the table it refers to is in a different query block
 * that is not visible.
 * See Expr.substitute() and related functions for details on the actual substitution.
 */
public final class ExprSubstitutionMap {
    private static final Logger LOG = LogManager.getLogger(ExprSubstitutionMap.class);

    private boolean checkAnalyzed = true;
    private boolean useNotCheckDescIdEquals = false;
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

    public void useNotCheckDescIdEquals() {
        useNotCheckDescIdEquals = true;
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

    public void putNoAnalyze(Expr lhsExpr, Expr rhsExpr) {
        lhs.add(lhsExpr);
        rhs.add(rhsExpr);
    }

    /**
     * Returns the expr mapped to lhsExpr or null if no mapping to lhsExpr exists.
     */
    public Expr get(Expr lhsExpr) {
        for (int i = 0; i < lhs.size(); ++i) {
            if (useNotCheckDescIdEquals) {
                if (lhsExpr.notCheckDescIdEquals(lhs.get(i))) {
                    return rhs.get(i);
                }
            } else {
                if (lhsExpr.equals(lhs.get(i))) {
                    return rhs.get(i);
                }
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
     * Returns lhs if the smap contains a mapping for rhsExpr.
     */
    public Expr mappingForRhsExpr(Expr rhsExpr) {
        for (int i = 0; i < rhs.size(); ++i) {
            if (rhs.get(i).equals(rhsExpr)) {
                return lhs.get(i);
            }
        }
        return null;
    }

    public void removeByLhsExpr(Expr lhsExpr) {
        for (int i = 0; i < lhs.size(); ++i) {
            if (lhs.get(i).equals(lhsExpr)) {
                lhs.remove(i);
                rhs.remove(i);
                break;
            }
        }
    }

    public void removeByRhsExpr(Expr rhsExpr) {
        for (int i = 0; i < rhs.size(); ++i) {
            if (rhs.get(i).equals(rhsExpr)) {
                lhs.remove(i);
                rhs.remove(i);
                break;
            }
        }
    }

    public void updateLhsExprs(List<Expr> lhsExprList) {
        lhs = lhsExprList;
    }

    public void updateRhsExprs(List<Expr> rhsExprList) {
        rhs = rhsExprList;
    }

    /**
     * Return a map  which is equivalent to applying f followed by g,
     * i.e., g(f()).
     * Always returns a non-null map.
     */
    public static ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g, Analyzer analyzer) {
        if (f == null && g == null) {
            return new ExprSubstitutionMap();
        }
        if (f == null) {
            return g;
        }
        if (g == null || g.size() == 0) {
            return f;
        }
        ExprSubstitutionMap result = new ExprSubstitutionMap();
        // f's substitution targets need to be substituted via g
        result.lhs = Expr.cloneList(f.lhs);
        result.rhs = Expr.substituteList(f.rhs, g, analyzer, true);

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
     * Returns the subtraction of two substitution maps.
     * f [A.id, B.id] g [A.id, C.id]
     * return: g-f [B,id, C,id]
     */
    public static ExprSubstitutionMap subtraction(ExprSubstitutionMap f, ExprSubstitutionMap g, Analyzer analyzer) {
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
        for (int i = 0; i < g.size(); i++) {
            if (f.containsMappingFor(g.lhs.get(i))) {
                result.put(f.get(g.lhs.get(i)), g.rhs.get(i));
                if (f.get(g.lhs.get(i)) instanceof SlotRef && g.rhs.get(i) instanceof SlotRef) {
                    analyzer.putEquivalentSlot(((SlotRef) g.rhs.get(i)).getSlotId(),
                            ((SlotRef) Objects.requireNonNull(f.get(g.lhs.get(i)))).getSlotId());
                }
            } else {
                result.put(g.lhs.get(i), g.rhs.get(i));
                if (g.lhs.get(i) instanceof SlotRef && g.rhs.get(i) instanceof SlotRef) {
                    analyzer.putEquivalentSlot(((SlotRef) g.rhs.get(i)).getSlotId(),
                            ((SlotRef) g.lhs.get(i)).getSlotId());
                }
            }
        }
        return result;
    }

    /**
     * Returns the replace of two substitution maps.
     * f [A.id, B.id] [A.age, B.age] [A.name, B.name] g [A.id, C.id] [B.age, C.age] [A.address, C.address]
     * return: [A.id, C,id] [A.age, C.age] [A.name, B.name] [A.address, C.address]
     */
    public static ExprSubstitutionMap composeAndReplace(ExprSubstitutionMap f, ExprSubstitutionMap g, Analyzer analyzer)
            throws AnalysisException {
        if (f == null && g == null) {
            return new ExprSubstitutionMap();
        }
        if (f == null) {
            return g;
        }
        if (g == null || g.size() == 0) {
            return f;
        }
        ExprSubstitutionMap result = new ExprSubstitutionMap();
        // compose f and g
        for (int i = 0; i < g.size(); i++) {
            boolean findGMatch = false;
            Expr gLhs = g.getLhs().get(i);
            for (int j = 0; j < f.size(); j++) {
                // case a->fn(b), b->c => a->fn(c)
                Expr fRhs = f.getRhs().get(j);
                if (fRhs.contains(gLhs)) {
                    Expr newRhs = fRhs.trySubstitute(g, analyzer, false);
                    if (!result.containsMappingFor(f.getLhs().get(j))) {
                        result.put(f.getLhs().get(j), newRhs);
                    }
                    findGMatch = true;
                }
            }
            if (!findGMatch) {
                result.put(g.getLhs().get(i), g.getRhs().get(i));
            }
        }
        // add remaining f
        for (int i = 0; i < f.size(); i++) {
            if (!result.containsMappingFor(f.lhs.get(i))) {
                result.put(f.lhs.get(i), f.rhs.get(i));
            }
        }
        return result;
    }

    /**
     * Returns the union of two substitution maps. Always returns a non-null map.
     */
    public static ExprSubstitutionMap combine(ExprSubstitutionMap f, ExprSubstitutionMap g) {
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
        substituteLhs(lhsSmap, analyzer, false);
    }

    public void substituteLhs(ExprSubstitutionMap lhsSmap, Analyzer analyzer, boolean preserveRootTypes) {
        lhs = Expr.substituteList(lhs, lhsSmap, analyzer, preserveRootTypes);
    }

    public List<Expr> getLhs() {
        return lhs;
    }

    public List<Expr> getRhs() {
        return rhs;
    }

    public int size() {
        return lhs.size();
    }

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
        // This method is very very time consuming, especially when planning large complex query.
        // So disable it by default.
        if (LOG.isDebugEnabled()) {
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
    }

    public void clear() {
        lhs.clear();
        rhs.clear();
    }

    @Override
    public ExprSubstitutionMap clone() {
        return new ExprSubstitutionMap(Expr.cloneList(lhs), Expr.cloneList(rhs));
    }

    public void reCalculateNullableInfoForSlotInRhs() {
        Preconditions.checkState(lhs.size() == rhs.size(), "lhs and rhs must be same size");
        for (int i = 0; i < rhs.size(); i++) {
            if (rhs.get(i) instanceof SlotRef) {
                ((SlotRef) rhs.get(i)).getDesc().setIsNullable(lhs.get(i).isNullable()
                        || ((SlotRef) rhs.get(i)).getDesc().getIsNullable());
            }
        }
    }
}
