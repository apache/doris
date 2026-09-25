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

package org.apache.doris.nereids.spm.capture;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.VariableMgr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * PlanCaptureFilter - capture filter logic (Phase 2, design doc 7.2.2).
 *
 * Decides whether an audit record should be auto-captured into a SPM baseline, using a
 * multi-level filter chain ordered by cost (cheapest first):
 *
 * - Level 1: basic checks on AuditEvent fields (query, nereids, not internal).
 * - Level 2: performance thresholds (query time / scan rows from Config).
 * - Level 3: multi-table JOIN (>= 2 distinct physical tables).
 * - Level 4: all involved tables still exist (catalog check, done separately so the
 *   pure logic stays unit-testable).
 * - Level 5: table name include / exclude regex from Config.
 *
 * Level 6 (duplicate detection against existing baselines) runs in PlanCaptureManager,
 * because the planSql is only known after the optimize + decompile step.
 */
public class PlanCaptureFilter {

    /** Table include regex; null matches all tables. */
    private final Pattern includePattern;

    /** Table exclude regex; null means no exclusion. */
    private final Pattern excludePattern;

    /** Minimum query time (ms) threshold (session variable plan_capture_min_query_time_ms). */
    private final long minQueryTimeMs;

    /** Minimum scan rows threshold (session variable plan_capture_min_scan_rows). */
    private final long minScanRows;

    /**
     * Constructs a filter reading the performance thresholds from the global session
     * variables (so `SET GLOBAL plan_capture_min_query_time_ms = ...` takes effect).
     *
     * @param includePattern table name include regex (empty / null matches all)
     * @param excludePattern table name exclude regex (empty / null excludes nothing)
     */
    public PlanCaptureFilter(String includePattern, String excludePattern) {
        this(includePattern, excludePattern,
                VariableMgr.getDefaultSessionVariable().getPlanCaptureMinQueryTimeMs(),
                VariableMgr.getDefaultSessionVariable().getPlanCaptureMinScanRows());
    }

    /**
     * Constructs a filter with explicit thresholds (used by tests and by callers that
     * already hold a SessionVariable).
     *
     * @param includePattern  table name include regex (empty / null matches all)
     * @param excludePattern  table name exclude regex (empty / null excludes nothing)
     * @param minQueryTimeMs  minimum query time (ms)
     * @param minScanRows     minimum scan rows
     */
    public PlanCaptureFilter(String includePattern, String excludePattern,
            long minQueryTimeMs, long minScanRows) {
        this.includePattern = compile(includePattern);
        this.excludePattern = compile(excludePattern);
        this.minQueryTimeMs = minQueryTimeMs;
        this.minScanRows = minScanRows;
    }

    private static Pattern compile(String regex) {
        if (regex == null || regex.isEmpty()) {
            return null;
        }
        return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    }

    /**
     * The pure, catalog-independent part of the filter chain (Levels 1, 2, 3, 5).
     *
     * @param event  the audit record
     * @param tables the distinct table full names extracted from the query (Level 3 / 5)
     * @return whether the query should be considered for capture
     */
    public boolean shouldCapture(AuditEvent event, List<String> tables) {
        // Level 1: basic checks (O(1) field comparisons)
        if (!event.isQuery) {
            return false;
        }
        if (!event.isNereids) {
            // SPM freezes a Nereids plan; legacy planner queries are skipped
            return false;
        }
        if (event.isInternal) {
            return false;
        }

        // Level 2: performance thresholds - a query is valuable only when it is slow
        // enough or scans enough rows
        if (event.queryTime < minQueryTimeMs && event.scanRows < minScanRows) {
            return false;
        }

        // Level 3: multi-table JOIN (>= 2 distinct physical tables)
        if (tables == null || tables.size() < 2) {
            return false;
        }

        // Level 5: table name regex
        return matchesTablePattern(tables);
    }

    /**
     * Level 4: whether all the given tables still exist in the CAPTURED namespace.
     *
     * Kept separate from shouldCapture so the pure filter chain can be unit tested
     * without a catalog. Three-part names resolve through CatalogMgr; two-part and
     * one-part names resolve against the audited query's catalog / database (never
     * against the internal catalog: the gate runs BEFORE processCandidate installs the
     * captured namespace, so resolving db.table internally would either filter out valid
     * external joins or accidentally validate them against an unrelated internal table).
     *
     * @param tables          distinct table full names (catalog.db.table / db.table / table)
     * @param capturedCatalog the audited query's catalog (may be empty -> internal)
     * @param capturedDb      the audited query's database (may be empty -> unknown)
     * @return true when every table resolves, false otherwise
     */
    public boolean allTablesExist(List<String> tables, String capturedCatalog, String capturedDb) {
        for (String fullName : tables) {
            if (!tableExists(fullName, capturedCatalog, capturedDb)) {
                return false;
            }
        }
        return true;
    }

    private boolean tableExists(String fullName, String capturedCatalog, String capturedDb) {
        try {
            String[] parts = fullName.split("\\.");
            String catalogName = capturedCatalog;
            String dbName = capturedDb;
            String tableName;
            if (parts.length >= 3) {
                catalogName = parts[parts.length - 3];
                dbName = parts[parts.length - 2];
                tableName = parts[parts.length - 1];
            } else if (parts.length == 2) {
                // db.table is relative to the current catalog
                dbName = parts[parts.length - 2];
                tableName = parts[parts.length - 1];
            } else {
                // plain table name without a db qualifier: treat as existing (cannot
                // verify unambiguously)
                return true;
            }
            CatalogIf catalog = (catalogName == null || catalogName.isEmpty())
                    ? Env.getCurrentInternalCatalog()
                    : Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
            if (catalog == null || dbName == null || dbName.isEmpty()) {
                return false;
            }
            DatabaseIf db = catalog.getDbNullable(dbName);
            if (db == null) {
                return false;
            }
            return db.getTableNullable(tableName) != null;
        } catch (Exception e) {
            // missing catalog (DdlException) or any resolution failure: not capturable
            return false;
        }
    }

    /**
     * Level 5: include / exclude regex over the extracted table names.
     *
     * @param tables distinct table names
     * @return true when at least one table matches the include pattern (if any) and no
     *         table matches the exclude pattern (if any)
     */
    private boolean matchesTablePattern(List<String> tables) {
        if (includePattern == null && excludePattern == null) {
            return true;
        }
        boolean hasInclude = includePattern == null
                || tables.stream().anyMatch(t -> includePattern.matcher(t).find());
        if (!hasInclude) {
            return false;
        }
        boolean hasExclude = excludePattern != null
                && tables.stream().anyMatch(t -> excludePattern.matcher(t).find());
        return !hasExclude;
    }

    /**
     * Extracts the distinct table names of a query by parsing it with the Nereids parser
     * and collecting catalog relations (LogicalCatalogRelation when a catalog is bound,
     * UnboundRelation for a plain parse without a catalog - e.g. audit-log SQL text).
     *
     * The unbound traversal is CTE-scope aware: at parse time a CTE consumer is also an
     * UnboundRelation, so WITH c AS (SELECT * FROM t1) SELECT * FROM c would otherwise
     * count c as a second physical table and admit the single-table workload the >= 2
     * table gate is meant to reject.
     *
     * @param sql the query text
     * @return the distinct full table names (sorted), or an empty list when the SQL
     *         cannot be parsed / contains no table
     */
    public static List<String> extractTableNames(String sql) {
        try {
            Plan parsed = new NereidsParser().parseSingle(sql);
            if (parsed instanceof LogicalPlan) {
                Set<LogicalCatalogRelation> relations =
                        PlanUtils.getLogicalScanFromRootPlan((LogicalPlan) parsed);
                Set<String> unboundNames = new LinkedHashSet<>();
                collectUnboundNames(parsed, Collections.emptySet(), unboundNames);
                return Stream.concat(
                                relations.stream()
                                        .map(LogicalCatalogRelation::getTable)
                                        .map(TableIf::getNameWithFullQualifiers),
                                unboundNames.stream())
                        .distinct()
                        .sorted()
                        .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
            }
        } catch (RuntimeException e) {
            // parse failure: cannot extract tables, treat as not capturable
        }
        return List.of();
    }

    /**
     * Collects the base-table names of every UnboundRelation reachable from the plan,
     * skipping references to the CTE aliases visible at their point (mirrors the
     * analyzer's CTE scoping: an alias body sees the earlier aliases plus itself only in
     * a recursive CTE; the main query sees all of them) and recursing into subquery
     * plans held by expressions.
     */
    private static void collectUnboundNames(Plan plan, Set<String> visibleCtes, Set<String> out) {
        if (plan instanceof UnboundRelation) {
            List<String> parts = ((UnboundRelation) plan).getNameParts();
            if (parts.size() == 1 && visibleCtes.contains(normalizeCteName(parts.get(0)))) {
                return; // reference to a CTE alias, not a physical table
            }
            out.add(String.join(".", parts));
            return;
        }
        if (plan instanceof LogicalCTE) {
            LogicalCTE<?> cte = (LogicalCTE<?>) plan;
            List<LogicalSubQueryAlias<Plan>> aliases = cte.getAliasQueries();
            Set<String> allVisible = new LinkedHashSet<>(visibleCtes);
            for (LogicalSubQueryAlias<Plan> alias : aliases) {
                allVisible.add(normalizeCteName(alias.getAlias()));
            }
            for (int i = 0; i < aliases.size(); i++) {
                Set<String> aliasScope = new LinkedHashSet<>(visibleCtes);
                for (int j = 0; j < i; j++) {
                    aliasScope.add(normalizeCteName(aliases.get(j).getAlias()));
                }
                if (cte.isRecursive() && aliases.get(i).isRecursiveCte()) {
                    aliasScope.add(normalizeCteName(aliases.get(i).getAlias()));
                }
                collectUnboundNames(aliases.get(i), Collections.unmodifiableSet(aliasScope), out);
            }
            if (cte.child(0) != null) {
                collectUnboundNames(cte.child(0), Collections.unmodifiableSet(allVisible), out);
            }
            return;
        }
        for (Plan child : plan.children()) {
            collectUnboundNames(child, visibleCtes, out);
        }
        for (Expression expr : plan.getExpressions()) {
            collectSubqueryNames(expr, visibleCtes, out);
        }
    }

    /** Recurses into the query plans of subquery expressions (IN / EXISTS / scalar). */
    private static void collectSubqueryNames(Expression expr, Set<String> visibleCtes, Set<String> out) {
        if (expr instanceof SubqueryExpr) {
            collectUnboundNames(((SubqueryExpr) expr).getQueryPlan(), visibleCtes, out);
        }
        for (Expression child : expr.children()) {
            collectSubqueryNames(child, visibleCtes, out);
        }
    }

    /** Mirrors the analyzer's CTE name comparison (lower_case_table_names). */
    private static String normalizeCteName(String name) {
        int lowerCaseTableNames = GlobalVariable.lowerCaseTableNames;
        return lowerCaseTableNames != 0 ? name.toLowerCase(Locale.ROOT) : name;
    }

    /**
     * Extracts the distinct table names of a query (alias of extractTableNames).
     *
     * @param sql the query text
     * @return the distinct table names
     */
    public static List<String> extractTables(String sql) {
        return extractTableNames(sql);
    }
}
