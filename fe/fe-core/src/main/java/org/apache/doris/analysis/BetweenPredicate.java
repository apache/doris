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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/BetweenPredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class describing between predicates. After successful analysis, we equal
 * the between predicate to a conjunctive/disjunctive compound predicate
 * to be handed to the backend.
 */
public class BetweenPredicate extends Predicate {
    private static final Logger LOG = LogManager.getLogger(BetweenPredicate.class);

    @SerializedName("inb")
    private boolean isNotBetween;

    private BetweenPredicate() {
        // use for serde only
    }

    // First child is the comparison expr which should be in [lowerBound, upperBound].
    public BetweenPredicate(Expr compareExpr, Expr lowerBound, Expr upperBound, boolean isNotBetween) {
        children.add(compareExpr);
        children.add(lowerBound);
        children.add(upperBound);
        this.isNotBetween = isNotBetween;
    }

    protected BetweenPredicate(BetweenPredicate other) {
        super(other);
        isNotBetween = other.isNotBetween;
    }

    @Override
    public Expr clone() {
        return new BetweenPredicate(this);
    }

    public boolean isNotBetween() {
        return isNotBetween;
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);
        if (children.get(0) instanceof Subquery
                && (children.get(1) instanceof Subquery || children.get(2) instanceof Subquery)) {
            throw new AnalysisException("Comparison between subqueries is not "
                    + "supported in a BETWEEN predicate: " + toSql());
        }
        // if children has subquery, it will be written and reanalyzed in the future.
        if (children.get(0) instanceof Subquery
                || children.get(1) instanceof Subquery
                || children.get(2) instanceof Subquery) {
            return;
        }
        analyzer.castAllToCompatibleType(children);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new IllegalStateException(
                "BetweenPredicate needs to be rewritten into a CompoundPredicate.");
    }

    @Override
    public String toSqlImpl() {
        String notStr = (isNotBetween) ? "NOT " : "";
        return children.get(0).toSql() + " " + notStr + "BETWEEN "
                + children.get(1).toSql() + " AND " + children.get(2).toSql();
    }

    @Override
    public String toDigestImpl() {
        String notStr = (isNotBetween) ? "NOT " : "";
        return children.get(0).toDigest() + " " + notStr + "BETWEEN "
                + children.get(1).toDigest() + " AND " + children.get(2).toDigest();
    }

    @Override
    public Expr clone(ExprSubstitutionMap sMap) {
        return new BetweenPredicate(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BetweenPredicate that = (BetweenPredicate) o;
        return isNotBetween == that.isNotBetween;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Boolean.hashCode(isNotBetween);
    }
}
