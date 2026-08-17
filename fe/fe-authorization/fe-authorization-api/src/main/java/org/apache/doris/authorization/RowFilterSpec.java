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

package org.apache.doris.authorization;

import java.util.Objects;

/**
 * One row-level security filter an authorization source applies to a table for one subject.
 *
 * <p>The payload is a <b>SQL boolean expression in Doris dialect</b> ({@code region = 'cn' AND dept IN
 * ('a','b')}), not a parsed expression tree. The engine parses it, checks that it is boolean, and injects it
 * as a filter above the scan. This is deliberate and mirrors Trino's {@code ViewExpression}: a row filter
 * originates as SQL text authored by a human in an external system (a Ranger policy, a {@code CREATE ROW
 * POLICY} statement), so text is its lossless native form. A structured tree would force every source to
 * embed a SQL parser, and cannot express what real policies contain - function calls above all.</p>
 *
 * <p>Value semantics are load bearing, not cosmetic: the SQL result cache decides "did the policies change?"
 * by comparing the specs recorded at plan time against the specs evaluated now. Identity equality there means
 * a freshly built - but identical - spec reads as a policy change and evicts the cache on every single
 * lookup. Hence this type is an immutable value with {@link #equals}/{@link #hashCode}, and every producer
 * must fold whatever version token it has into {@link #getPolicyIdent()} so that a genuinely updated policy
 * does compare unequal.</p>
 */
public final class RowFilterSpec {

    private final String policyIdent;
    private final String filterSql;
    private final RowFilterMergeType mergeType;

    /**
     * @param policyIdent identifies the policy that produced this filter, for auditing and for change
     *         detection. The spec as a whole has to compare unequal once the policy changes; carry the
     *         version here when nothing else moves (e.g. {@code "<policyId>:<version>"} for a source whose
     *         policies are edited in place)
     * @param filterSql a boolean SQL expression in Doris dialect over the table's columns
     * @param mergeType how this filter combines with the table's other filters
     */
    public RowFilterSpec(String policyIdent, String filterSql, RowFilterMergeType mergeType) {
        this.policyIdent = requireNonBlank(policyIdent, "policyIdent");
        this.filterSql = requireNonBlank(filterSql, "filterSql");
        this.mergeType = Objects.requireNonNull(mergeType, "mergeType is required");
    }

    /** Creates a restrictive (ANDed) filter, the default for an authorization source. */
    public static RowFilterSpec restrictive(String policyIdent, String filterSql) {
        return new RowFilterSpec(policyIdent, filterSql, RowFilterMergeType.RESTRICTIVE);
    }

    public String getPolicyIdent() {
        return policyIdent;
    }

    public String getFilterSql() {
        return filterSql;
    }

    public RowFilterMergeType getMergeType() {
        return mergeType;
    }

    private static String requireNonBlank(String value, String field) {
        Objects.requireNonNull(value, field + " is required");
        if (value.trim().isEmpty()) {
            // A blank filter would silently degrade to "no restriction" once parsed, i.e. it would widen
            // access. Reject at construction so a broken policy fails loudly at its source.
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowFilterSpec)) {
            return false;
        }
        RowFilterSpec that = (RowFilterSpec) o;
        return policyIdent.equals(that.policyIdent)
                && filterSql.equals(that.filterSql)
                && mergeType == that.mergeType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(policyIdent, filterSql, mergeType);
    }

    @Override
    public String toString() {
        return "RowFilterSpec{policyIdent='" + policyIdent + "', filterSql='" + filterSql
                + "', mergeType=" + mergeType + '}';
    }
}
