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
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.VariableMgr;

import java.util.ArrayList;
import java.util.List;
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
     * Level 4: whether all the given tables still exist in the catalog.
     *
     * Kept separate from shouldCapture so the pure filter chain can be unit
     * tested without a catalog.
     *
     * @param tables distinct table full names (catalog.db.table or db.table)
     * @return true when every table resolves, false otherwise
     */
    public boolean allTablesExist(List<String> tables) {
        for (String fullName : tables) {
            if (!tableExists(fullName)) {
                return false;
            }
        }
        return true;
    }

    private boolean tableExists(String fullName) {
        try {
            String[] parts = fullName.split("\\.");
            if (parts.length >= 2) {
                String dbName = parts[parts.length - 2];
                String tableName = parts[parts.length - 1];
                DatabaseIf db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
                if (db == null) {
                    return false;
                }
                return db.getTable(tableName).isPresent();
            }
            if (parts.length == 1) {
                // plain table name without a db qualifier: treat as existing (cannot
                // verify unambiguously)
                return true;
            }
        } catch (RuntimeException e) {
            return false;
        }
        return false;
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
                Set<UnboundRelation> unboundRelations =
                        parsed.collect(UnboundRelation.class::isInstance);
                return Stream.concat(
                                relations.stream()
                                        .map(LogicalCatalogRelation::getTable)
                                        .map(TableIf::getNameWithFullQualifiers),
                                unboundRelations.stream().map(UnboundRelation::getTableName))
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
     * Extracts the distinct table names of a query (alias of extractTableNames).
     *
     * @param sql the query text
     * @return the distinct table names
     */
    public static List<String> extractTables(String sql) {
        return extractTableNames(sql);
    }
}
