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

package org.apache.doris.filesystem.spi;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Numeric-range normalization shared by object storage and Hadoop glob listing. */
public final class GlobPattern {
    private static final int MAX_EXPANSIONS = 10000;
    private static final Pattern RANGE = Pattern.compile("(-?\\d+)\\.\\.(-?\\d+)");
    private static final Pattern SIMPLE_RANGE = Pattern.compile("\\d+\\.\\.\\d+");

    private GlobPattern() {
    }

    /**
     * Expands {@code {N..M}} numeric range syntax in a glob pattern to the equivalent
     * comma-separated alternation {@code {N,N+1,...,M}} understood by Hadoop and object-storage globs.
     * Supports simple non-negative ranges like {@code {1..3}}, reverse ranges like {@code {3..1}},
     * and mixed comma-separated patterns like {@code {1..2,3,1..3}} or {@code {Refrain,1..3}}.
     * For simple (no-comma) brace groups, only non-negative ranges are expanded;
     * negative-start ranges like {@code {-1..1}} are left unchanged.
     * Duplicate values are removed. For example:
     * <ul>
     *   <li>{@code data_{1..3}.csv} → {@code data_{1,2,3}.csv}</li>
     *   <li>{@code data_{1..2,3,1..3}.csv} → {@code data_{1,2,3}.csv}</li>
     *   <li>{@code data_{Refrain,1..3}.csv} → {@code data_{Refrain,1,2,3}.csv}</li>
     *   <li>{@code data_{-1..1}.csv} → unchanged (no expansion)</li>
     * </ul>
     */
    public static String expandNumericRanges(String pattern) {
        StringBuilder result = new StringBuilder();
        Deque<BraceGroup> groups = new ArrayDeque<>();
        ExpansionBudget budget = new ExpansionBudget();
        boolean characterClass = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            StringBuilder output = groups.isEmpty() ? result : groups.peek().segment;
            // Escaped braces and commas are literal characters, even inside an alternative.
            // Character classes likewise must not change the surrounding brace structure.
            if (c == '\\') {
                output.append(c);
                if (i + 1 < pattern.length()) {
                    output.append(pattern.charAt(++i));
                }
            } else if (characterClass) {
                output.append(c);
                if (c == ']') {
                    characterClass = false;
                }
            } else if (c == '[') {
                characterClass = true;
                output.append(c);
            } else if (c == '{') {
                groups.push(new BraceGroup());
            } else if (c == ',' && !groups.isEmpty()) {
                groups.peek().finishSegment();
            } else if (c == '}' && !groups.isEmpty()) {
                BraceGroup group = groups.pop();
                group.finishSegment();
                String expanded = group.expand(budget);
                (groups.isEmpty() ? result : groups.peek().segment).append('{').append(expanded).append('}');
            } else {
                output.append(c);
            }
        }
        while (!groups.isEmpty()) {
            BraceGroup group = groups.pop();
            group.finishSegment();
            (groups.isEmpty() ? result : groups.peek().segment)
                    .append('{').append(String.join(",", group.segments));
        }
        return result.toString();
    }

    private static final class ExpansionBudget {
        private long combinations = 1;
        private long expandedValues;
    }

    private static final class BraceGroup {
        private final List<String> segments = new ArrayList<>();
        private final StringBuilder segment = new StringBuilder();

        private void finishSegment() {
            segments.add(segment.toString());
            segment.setLength(0);
        }

        private String expand(ExpansionBudget budget) {
            if (segments.size() == 1 && !SIMPLE_RANGE.matcher(segments.get(0)).matches()) {
                return segments.get(0);
            }
            Set<String> values = new LinkedHashSet<>();
            boolean expanded = false;
            for (String alternative : segments) {
                Matcher range = RANGE.matcher(alternative.trim());
                if (range.matches()) {
                    // Count before materializing, and use long arithmetic so an int endpoint
                    // cannot wrap the loop or turn a huge requested range into a small count.
                    long from = Integer.parseInt(range.group(1));
                    long to = Integer.parseInt(range.group(2));
                    budget.expandedValues += Math.abs(to - from) + 1;
                    checkLimit(budget.expandedValues);
                    long step = from <= to ? 1 : -1;
                    for (long value = from; step > 0 ? value <= to : value >= to; value += step) {
                        values.add(Long.toString(value));
                    }
                    expanded = true;
                } else {
                    values.add(alternative.trim());
                }
            }
            if (expanded) {
                budget.combinations *= values.size();
                checkLimit(budget.combinations);
                return String.join(",", values);
            }
            return String.join(",", segments);
        }
    }

    private static void checkLimit(long count) {
        if (count > MAX_EXPANSIONS) {
            throw new IllegalArgumentException("Numeric glob range expansion exceeds " + MAX_EXPANSIONS + " values");
        }
    }

}
