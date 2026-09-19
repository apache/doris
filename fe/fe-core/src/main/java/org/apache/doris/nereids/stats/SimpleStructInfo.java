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

package org.apache.doris.nereids.stats;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Human readable (simplified) rendering of a hbo struct info canonical string.
 *
 * <p>The canonical string is the fingerprint input and stays the authoritative description; the
 * simplified string is only meant to help a user locate an entry, so it drops everything that
 * cannot be told apart at a glance and keeps the shape:
 * <ul>
 *   <li>scan: {@code S{internal.hbo_test.t,v2,r1000}} becomes {@code S{hbo_test.t}} - the catalog
 *       is dropped (except for non internal catalogs, which are kept because they identify the
 *       table), the data state of the scan is dropped (the visible version and the scanned rows,
 *       which only annotate the entry and never take part in its fingerprint), the pruned partition
 *       count is kept ({@code ,p1/2}) and a {@code #k} suffix is added only when the same table
 *       appears more than once in the struct;</li>
 *   <li>filter: predicates are written with operator symbols ({@code col(internal.hbo_test.x.b) =
 *       lit(1:INT)} becomes {@code x.b = 1}), {@code ;} means AND, {@code |} means OR, literals are
 *       written as their value (or as {@code *} for the constant agnostic granularity), and a
 *       column keeps its qualifier only when it is needed to tell two occurrences apart;</li>
 *   <li>join: only the list of the chain leaves is kept, join type and join conditions are dropped
 *       ({@code J{S{hbo_test.t1}, S{hbo_test.t2}, S{hbo_test.t3}}}), because the chain is already
 *       flattened and the remaining differences are visible in the canonical form;</li>
 *   <li>aggregation: only the grouping keys and the child are kept, the aggregate functions and the
 *       output expressions are dropped (they cannot change the output row count).</li>
 * </ul>
 * Two entries may therefore share one simplified string while having different canonical strings;
 * {@code HBO SHOW STATISTICS FULL} prints the canonical form to tell them apart.
 *
 * <p>Because the simplified form is a pure function of the canonical string, it never needs to be
 * persisted: it is derived on demand, so it can never go stale.
 */
public final class SimpleStructInfo {
    private static final Map<String, String> BINARY_OPERATORS = ImmutableMap.<String, String>builder()
            .put("EqualTo", "=")
            .put("NotEqualTo", "!=")
            .put("LessThan", "<")
            .put("LessThanEqual", "<=")
            .put("GreaterThan", ">")
            .put("GreaterThanEqual", ">=")
            .put("NullSafeEqual", "<=>")
            .build();
    private static final String CATALOG_NAME = "internal";
    private static final char AND = ';';
    private static final String OR = "|";

    private final String canonical;
    private final Set<String> tableNames = new HashSet<>();
    private final Map<String, Integer> scanTokenCount = new LinkedHashMap<>();
    private final Map<String, Integer> scanTokenSeen = new HashMap<>();
    private int pos;

    private SimpleStructInfo(String canonical) {
        this.canonical = canonical;
    }

    /**
     * Render the simplified form of a canonical struct info string. Text which is not a canonical
     * struct info (a row of the internal table written by an older version, or a hand written
     * literal) is returned unchanged instead of printing something misleading.
     */
    public static String render(String canonical) {
        if (canonical == null || canonical.isEmpty()) {
            return "";
        }
        SimpleStructInfo renderer = new SimpleStructInfo(canonical);
        renderer.collectScans();
        try {
            String simple = renderer.renderNode(null);
            return renderer.pos == canonical.length() ? simple : canonical;
        } catch (RuntimeException e) {
            return canonical;
        }
    }

    private void collectScans() {
        int index = 0;
        while ((index = canonical.indexOf("S{", index)) >= 0) {
            int end = canonical.indexOf('}', index);
            if (end < 0) {
                return;
            }
            String header = canonical.substring(index + 2, end);
            String token = scanToken(header);
            scanTokenCount.merge(token, 1, Integer::sum);
            tableNames.add(tableName(header));
            index = end;
        }
    }

    // ------------------------------------------------------------------------------------------
    // node rendering
    // ------------------------------------------------------------------------------------------

    /**
     * Render one node, starting at {@link #pos}.
     *
     * @param parent the logical operator of the enclosing node ({@code null} when top level), used
     *               to decide whether a nested AND / OR needs parentheses
     */
    private String renderNode(String parent) {
        char kind = canonical.charAt(pos++);
        String header = readBraced();
        if (kind == 'S') {
            return renderScan(header);
        }
        expect('(');
        if (kind == 'J') {
            List<String> children = new ArrayList<>();
            children.add(renderNode(parent));
            while (canonical.charAt(pos) == AND) {
                pos++;
                children.add(renderNode(parent));
            }
            expect(')');
            // join type and join conditions are dropped: the leaves and the canonical form carry
            // the differences a user has to tell apart
            return "J{" + String.join(", ", children) + "}";
        }
        String child = renderNode(parent);
        expect(')');
        if (kind == 'F') {
            return "F{" + renderPredicates(header, parent) + "}(" + child + ")";
        }
        if (kind == 'A') {
            // gb:<group by keys>
            int colon = header.indexOf(':');
            String groupBy = colon < 0 ? header : header.substring(colon + 1);
            return "A{" + renderPredicates(groupBy, parent) + "}(" + child + ")";
        }
        throw new IllegalStateException("unknown hbo struct info node: " + kind);
    }

    private String renderScan(String header) {
        String token = scanToken(header);
        String[] parts = header.split(",");
        StringBuilder sb = new StringBuilder("S{").append(simplifiedTableName(parts[0]));
        Integer count = scanTokenCount.get(token);
        if (count != null && count > 1) {
            sb.append('#').append(scanTokenSeen.merge(token, 1, Integer::sum) - 1);
        }
        for (int i = 1; i < parts.length; i++) {
            // keep the pruned partition count, drop the visible version
            if (parts[i].startsWith("p")) {
                sb.append(',').append(parts[i]);
            }
        }
        return sb.append('}').toString();
    }

    // ------------------------------------------------------------------------------------------
    // expression rendering
    // ------------------------------------------------------------------------------------------

    private String renderPredicates(String expressions, String parent) {
        List<String> rendered = new ArrayList<>();
        for (String conjunct : splitTopLevel(expressions, AND)) {
            if (!conjunct.trim().isEmpty()) {
                rendered.add(renderExpression(conjunct, parent));
            }
        }
        return String.join("; ", rendered);
    }

    /**
     * Render one canonical expression ({@code ClassName(children)}, {@code col(qualifier.name)} or
     * {@code lit(value:type)}) with operator symbols.
     */
    private String renderExpression(String expression, String parent) {
        String text = expression.trim();
        if (text.startsWith("col(") && text.endsWith(")")) {
            return renderColumn(text.substring(4, text.length() - 1));
        }
        if (text.startsWith("lit(") && text.endsWith(")")) {
            return renderLiteral(text.substring(4, text.length() - 1));
        }
        int open = text.indexOf('(');
        if (open < 0) {
            return text;
        }
        String name = text.substring(0, open);
        // only nested AND / OR groups need parentheses, comparisons never do; the operands of any
        // other expression are rendered without a logical context
        String childParent = "And".equals(name) ? "and" : "Or".equals(name) ? "or" : null;
        List<String> arguments = new ArrayList<>();
        for (String argument : splitTopLevel(text.substring(open + 1, text.length() - 1), ',')) {
            arguments.add(renderExpression(argument, childParent));
        }
        String operator = BINARY_OPERATORS.get(name);
        if (operator != null && arguments.size() == 2) {
            return arguments.get(0) + " " + operator + " " + arguments.get(1);
        }
        if ("And".equals(name)) {
            String joined = String.join("; ", arguments);
            return "or".equals(parent) ? "(" + joined + ")" : joined;
        }
        if ("Or".equals(name)) {
            String joined = String.join(" " + OR + " ", arguments);
            return "and".equals(parent) ? "(" + joined + ")" : joined;
        }
        if ("Not".equals(name) && arguments.size() == 1) {
            return "!(" + arguments.get(0) + ")";
        }
        return name + "(" + String.join(", ", arguments) + ")";
    }

    /**
     * A column keeps its qualifier unless the struct describes a single table and the qualifier is
     * that table: a single table struct is already identified by its scan token, while an alias (or
     * a second table) is needed to tell two occurrences apart.
     */
    private String renderColumn(String qualifiedName) {
        int lastDot = qualifiedName.lastIndexOf('.');
        if (lastDot < 0) {
            return qualifiedName;
        }
        String qualifier = qualifiedName.substring(0, lastDot);
        String column = qualifiedName.substring(lastDot + 1);
        int qualifierDot = qualifier.lastIndexOf('.');
        String owner = qualifierDot < 0 ? qualifier : qualifier.substring(qualifierDot + 1);
        if (tableNames.size() == 1 && tableNames.contains(owner)) {
            return column;
        }
        return owner + "." + column;
    }

    private String renderLiteral(String literal) {
        // lit(*) for the constant agnostic granularity, lit(value:type) otherwise: the type is
        // dropped, the value is kept
        int lastColon = literal.lastIndexOf(':');
        return lastColon < 0 ? literal : literal.substring(0, lastColon);
    }

    // ------------------------------------------------------------------------------------------
    // string helpers
    // ------------------------------------------------------------------------------------------

    /** Read the content of the bracketed group which starts at {@link #pos}, advancing past it. */
    private String readBraced() {
        char open = canonical.charAt(pos);
        char close = open == '{' ? '}' : ')';
        expect(open);
        // the opening bracket is already consumed but it is still open
        int depth = 1;
        int start = pos;
        while (pos < canonical.length()) {
            char c = canonical.charAt(pos);
            if (c == open || c == '(' || c == '[') {
                depth++;
            } else if (c == close || c == ')' || c == ']') {
                depth--;
                if (depth == 0) {
                    String content = canonical.substring(start, pos);
                    pos++;
                    return content;
                }
            }
            pos++;
        }
        throw new IllegalStateException("unbalanced hbo struct info: " + canonical);
    }

    private void expect(char expected) {
        if (canonical.charAt(pos) != expected) {
            throw new IllegalStateException("expect '" + expected + "' at " + pos + " in " + canonical);
        }
        pos++;
    }

    /** Split on a separator which is not nested in brackets. */
    private static List<String> splitTopLevel(String text, char separator) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(' || c == '[' || c == '{') {
                depth++;
            } else if (c == ')' || c == ']' || c == '}') {
                depth--;
            } else if (c == separator && depth == 0) {
                parts.add(text.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(text.substring(start));
        return parts;
    }

    /**
     * The scan token without its baseline (visible version, scanned rows): what makes two scan
     * tokens interchangeable. The partition count is kept, because it says which part of the table
     * the entry was measured on.
     */
    private static String scanToken(String header) {
        HboScanDescriptor descriptor = HboScanDescriptor.parse(header);
        return descriptor.isPartitionSelectionComplete() ? descriptor.getTable()
                : descriptor.getTable() + ",p" + descriptor.getSelectedPartitions()
                        + "/" + descriptor.getTotalPartitions();
    }

    private static String tableName(String header) {
        String fullName = header.split(",")[0];
        int lastDot = fullName.lastIndexOf('.');
        return lastDot < 0 ? fullName : fullName.substring(lastDot + 1);
    }

    /** `internal` is the default catalog and is dropped; other catalogs identify the table. */
    private static String simplifiedTableName(String fullName) {
        String prefix = CATALOG_NAME + ".";
        return fullName.startsWith(prefix) ? fullName.substring(prefix.length()) : fullName;
    }
}
