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

package org.apache.doris.nereids.spm.builder;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.ExprId;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Intermediate representation of the SPM physical-plan decompiler.
 *
 * A container for the SQL fragments assembled while SPMPlan2SQLBuilder walks a physical
 * plan tree. Each physical operator (Scan, Join, Aggregate, TopN, ...) produces a
 * SQLRelation that represents one complete SELECT block. A parent operator references a
 * child relation through toRelationSQL():
 *
 * - relationName == null (inline): return the from field directly (e.g. "t1")
 * - relationName != null (after newAlias): wrap as a (SELECT ...) t_N subquery
 *
 * This class models the relation rendering state of SPMPlan2SQLBuilder (design doc 6.2.1).
 * Key fields: columnNames (ExprId -> SQL reference name), from / where / groupBy / having /
 * orderBy / limit / groupings, selects (the SELECT list), and relationName (subquery alias).
 */
public class SQLRelation {

    /**
     * Per-decompile table-alias sequence (t_0, t_1, ...). A relation only needs its
     * alias to be unique WITHIN the one SQL it belongs to, so the sequence is reset at
     * the start of each decompile (SPMPlan2SQLBuilder#toSQL) instead of growing forever
     * process-wide - aliases stay small and the decompiled SQL is reproducible.
     * ThreadLocal keeps concurrent decompiles on different FE worker threads isolated.
     * A standalone SQLRelation (expression printing / tests, which never call
     * newAlias()) lazily gets its own per-thread sequence.
     */
    private static final ThreadLocal<AtomicInteger> TABLE_ALIAS =
            ThreadLocal.withInitial(AtomicInteger::new);

    /** Column name mapping: ExprId -> SQL reference name (e.g. "c_1" or "sum(c_2)"). */
    private final Map<ExprId, String> columnNames = Maps.newHashMap();

    /** WITH clause fragments (CTE decompile output); null means no CTE. */
    private List<String> cte = null;

    /** SELECT list: (column ExprId, SQL text of that column). An empty list makes toSQL() output "*". */
    private List<Pair<ExprId, String>> selects = List.of();

    /** Hints such as SET_VAR (reserved; Phase 1 only keeps the field). */
    private String hints = "";

    /** FROM clause text. */
    private String from = "";

    /** WHERE clause text. */
    private String where = "";

    /** GROUP BY clause text. */
    private String groupBy = "";

    /** HAVING clause text. */
    private String having = "";

    /** ORDER BY clause text. */
    private String orderBy = "";

    /** LIMIT clause text in the form "offset, limit". */
    private String limit = "";

    /** GROUPING SETS expressions (written by PhysicalRepeat, consumed by the parent global aggregate). */
    private String groupings = "";

    /** Subquery alias (t_N). null means inline (no subquery nesting). */
    private String relationName = null;

    /** All column names of the table (to avoid JOIN column-name conflicts; reserved in Phase 1). */
    private List<String> reserveNames = null;

    /**
     * Registers an explicit reference name (e.g. a local aggregate registers
     * "sum(c_2)" under the id of column 10).
     *
     * @param exprId column id
     * @param alias  SQL reference name
     * @return the registered alias
     */
    public String registerRef(ExprId exprId, String alias) {
        columnNames.put(exprId, alias);
        return alias;
    }

    /**
     * Registers a normalized reference name (default c_ExprId).
     *
     * @param exprId column id
     * @return the generated c_N alias
     */
    public String registerRef(ExprId exprId) {
        String ref = "c_" + exprId;
        columnNames.put(exprId, ref);
        return ref;
    }

    /**
     * Allocates a new alias and marks this relation as needing subquery wrapping.
     *
     * @return the new alias t_N
     */
    public String newAlias() {
        relationName = "t_" + TABLE_ALIAS.get().getAndIncrement();
        return relationName;
    }

    /**
     * Returns the alias used when this relation is referenced as a relation
     * expression by a parent operator. When relationName == null (a plain inline
     * table name), the from field itself (e.g. "t1") is the alias.
     *
     * @return the relation alias
     */
    public String getRelationAlias() {
        return relationName == null ? from : relationName;
    }

    /**
     * Returns a LEGAL qualifier for this relation's columns, wrapping the relation when
     * its FROM fragment cannot be used as a qualifier prefix. A plain table reference
     * (catalog.db.table) is its own qualifier, but a composite fragment - a scan carrying
     * modifiers ("internal.db.t PARTITION(p1)", "... TABLESAMPLE(...)", a lateral-view
     * chain), a table-valued function call, an already wrapped subquery or a whole join -
     * would produce invalid references such as "internal.db.t PARTITION(p1).id". Those
     * are wrapped as a derived table ("(SELECT * FROM ...) t_N") and qualified with the
     * generated alias, which is what toRelationSQL() emits for the parent's FROM.
     *
     * @return the qualifier to prefix this relation's columns with
     */
    public String ensureQualifierAlias() {
        if (relationName != null) {
            return relationName;
        }
        if (isPlainQualifier(from)) {
            return from;
        }
        newAlias();
        return relationName;
    }

    /** A FROM fragment usable as a column qualifier: dot-separated plain identifiers. */
    private static boolean isPlainQualifier(String from) {
        return !from.isEmpty() && from.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*");
    }

    /**
     * Returns the SQL fragment that a parent operator can reference; this is where
     * subquery nesting is generated. Two branches (design doc 6.2.1):
     *
     * - relationName == null: inline, return from directly (e.g. "t1")
     * - otherwise: wrap as the (SELECT ...) t_N subquery
     *
     * @return the fragment that can be embedded into a parent FROM clause
     */
    public String toRelationSQL() {
        if (relationName == null) {
            if (from.isEmpty()) {
                // A FROM-less relation (a reduced one-row plan such as "SELECT 7 AS id")
                // must be wrapped before it can sit in a join or as a lateral-view input:
                // returning the empty FROM fragment would leave "... CROSS JOIN" behind
                // and the frozen SQL would no longer parse. Allocate the alias lazily.
                newAlias();
                return "(" + toSQL() + ") " + relationName;
            }
            return from;
        }
        return "(" + toSQL() + ") " + relationName;
    }

    /**
     * Outputs the complete SELECT statement (WITH, SELECT, FROM, WHERE, GROUP BY,
     * HAVING, ORDER BY, LIMIT).
     *
     * @return the complete SELECT SQL
     */
    public String toSQL() {
        StringBuilder sql = new StringBuilder();
        if (cte != null && !cte.isEmpty()) {
            sql.append("WITH ").append(String.join(", ", cte)).append(" ");
        }
        sql.append("SELECT ");
        if (!hints.isEmpty()) {
            sql.append(hints);
        }
        if (selects.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(selects.stream().map(Pair::value).collect(Collectors.joining(", ")));
        }
        if (!from.isEmpty()) {
            sql.append(" FROM ").append(from);
        }
        if (!where.isEmpty()) {
            sql.append(" WHERE ").append(where);
        }
        if (!groupBy.isEmpty()) {
            sql.append(" GROUP BY ").append(groupBy);
        }
        if (!having.isEmpty()) {
            sql.append(" HAVING ").append(having);
        }
        if (!orderBy.isEmpty()) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        if (!limit.isEmpty()) {
            sql.append(" LIMIT ").append(limit);
        }
        return sql.toString();
    }

    /**
     * Whether this relation carries its own query block (SELECT list / WHERE / GROUP BY /
     * HAVING / ORDER BY / LIMIT / GROUPING SETS / WITH) on top of its FROM. Such a
     * relation MUST be wrapped as a subquery before another FROM-level clause (e.g.
     * LATERAL VIEW) is attached: keeping its clauses on the outer relation would move
     * them AFTER the new clause and change the result (LIMIT 10 would then limit the
     * exploded rows instead of the lateral-view input, a GROUP BY would regroup the
     * generator output, ...).
     *
     * @return true when a subquery wrapper is required to stay semantically identical
     */
    public boolean hasOwnBlock() {
        return !selects.isEmpty()
                || !where.isEmpty()
                || !groupBy.isEmpty()
                || !having.isEmpty()
                || !orderBy.isEmpty()
                || !limit.isEmpty()
                || !groupings.isEmpty()
                || (cte != null && !cte.isEmpty());
    }

    // ==================== getters / setters (used by SPMPlan2SQLBuilder) ====================

    public Map<ExprId, String> getColumnNames() {
        return columnNames;
    }

    public List<String> getCte() {
        return cte;
    }

    public void setCte(List<String> cte) {
        this.cte = cte;
    }

    public List<Pair<ExprId, String>> getSelects() {
        return selects;
    }

    public void setSelects(List<Pair<ExprId, String>> selects) {
        this.selects = selects;
    }

    public String getHints() {
        return hints;
    }

    public void setHints(String hints) {
        this.hints = hints;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
    }

    public String getHaving() {
        return having;
    }

    public void setHaving(String having) {
        this.having = having;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }

    public String getGroupings() {
        return groupings;
    }

    public void setGroupings(String groupings) {
        this.groupings = groupings;
    }

    public String getRelationName() {
        return relationName;
    }

    public void setRelationName(String relationName) {
        this.relationName = relationName;
    }

    public List<String> getReserveNames() {
        return reserveNames;
    }

    public void setReserveNames(List<String> reserveNames) {
        this.reserveNames = reserveNames;
    }

    // ==================== table alias counter ====================

    /** Allocates a fresh relation alias (t_N) without marking any relation as needing
     * subquery wrapping. Used for CTE definition names in the emitted WITH clause. */
    public static String newTableAlias() {
        return "t_" + TABLE_ALIAS.get().getAndIncrement();
    }

    /** Resets the per-decompile alias sequence (called at the start of each decompile
     * and by tests that assert on t_N values). */
    public static void resetAliasCounter() {
        TABLE_ALIAS.get().set(0);
    }
}
