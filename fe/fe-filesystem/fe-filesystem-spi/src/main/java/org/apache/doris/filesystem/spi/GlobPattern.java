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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Numeric-range normalization shared by object storage and Hadoop glob listing. */
public final class GlobPattern {
    private static final int MAX_EXPANSIONS = 10000;
    private static final Pattern RANGE = Pattern.compile("(-?\\d+)\\.\\.(-?\\d+)");
    private static final Pattern SIMPLE_RANGE = Pattern.compile("\\d+\\.\\.\\d+");
    private static final Pattern BRACE_GROUP = Pattern.compile("\\{([^{}]*)\\}");

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
        Matcher groups = BRACE_GROUP.matcher(pattern);
        StringBuffer result = new StringBuffer();
        long combinations = 1;
        long expandedValues = 0;
        int scanIndex = 0;
        boolean characterClass = false;
        while (groups.find()) {
            while (scanIndex < groups.start()) {
                char c = pattern.charAt(scanIndex++);
                if (c == '\\') {
                    scanIndex++;
                } else if (c == '[') {
                    characterClass = true;
                } else if (c == ']') {
                    characterClass = false;
                }
            }
            if (characterClass || scanIndex > groups.start()) {
                continue;
            }
            String content = groups.group(1);
            if (!content.contains(",") && !SIMPLE_RANGE.matcher(content).matches()) {
                continue;
            }
            String[] segments = content.split(",", -1);
            Set<String> values = new LinkedHashSet<>();
            boolean expanded = false;
            for (String segment : segments) {
                Matcher range = RANGE.matcher(segment.trim());
                if (range.matches()) {
                    // Count before materializing, and use long arithmetic so an int endpoint
                    // cannot wrap the loop or turn a huge requested range into a small count.
                    long from = Integer.parseInt(range.group(1));
                    long to = Integer.parseInt(range.group(2));
                    expandedValues += Math.abs(to - from) + 1;
                    checkLimit(expandedValues);
                    long step = from <= to ? 1 : -1;
                    for (long value = from; step > 0 ? value <= to : value >= to; value += step) {
                        values.add(Long.toString(value));
                    }
                    expanded = true;
                } else {
                    values.add(segment.trim());
                }
            }
            if (expanded) {
                combinations *= values.size();
                checkLimit(combinations);
                groups.appendReplacement(result, Matcher.quoteReplacement("{" + String.join(",", values) + "}"));
            }
        }
        groups.appendTail(result);
        return result.toString();
    }

    private static void checkLimit(long count) {
        if (count > MAX_EXPANSIONS) {
            throw new IllegalArgumentException("Numeric glob range expansion exceeds " + MAX_EXPANSIONS + " values");
        }
    }

}
