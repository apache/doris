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

package org.apache.doris.cloud;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ON TABLES clause filter for table-level event-driven warmup.
 *
 * Semantics: INCLUDE union − EXCLUDE union.
 * A table is warmed up if it matches any INCLUDE rule and does not match any EXCLUDE rule.
 */
public class OnTablesFilter {

    /**
     * A single INCLUDE or EXCLUDE rule with a glob pattern compiled to a Java regex.
     */
    public static class TableFilterRule {
        public enum RuleType {
            INCLUDE,
            EXCLUDE
        }

        private final RuleType ruleType;
        private final String rawPattern;
        private final Pattern compiledPattern;

        public TableFilterRule(RuleType ruleType, String globPattern) {
            this.ruleType = ruleType;
            this.rawPattern = globPattern;
            this.compiledPattern = compileGlob(globPattern);
        }

        /**
         * Compile a glob pattern to an anchored Java regex.
         * Glob: '*' matches any characters, '?' matches a single character,
         *        '.' and other regex metacharacters are treated as literals.
         */
        private static Pattern compileGlob(String glob) {
            StringBuilder regex = new StringBuilder("^");
            for (char c : glob.toCharArray()) {
                switch (c) {
                    case '*':
                        regex.append(".*");
                        break;
                    case '?':
                        regex.append(".");
                        break;
                    case '.':
                    case '(':
                    case ')':
                    case '[':
                    case ']':
                    case '{':
                    case '}':
                    case '\\':
                    case '^':
                    case '$':
                    case '|':
                    case '+':
                        regex.append('\\').append(c);
                        break;
                    default:
                        regex.append(c);
                }
            }
            regex.append("$");
            return Pattern.compile(regex.toString());
        }

        public boolean matches(String fullTableName) {
            return compiledPattern.matcher(fullTableName).matches();
        }

        public RuleType getRuleType() {
            return ruleType;
        }

        public String getRawPattern() {
            return rawPattern;
        }
    }

    private final List<TableFilterRule> includeRules;
    private final List<TableFilterRule> excludeRules;

    public OnTablesFilter(List<TableFilterRule> rules) {
        List<TableFilterRule> includes = new ArrayList<>();
        List<TableFilterRule> excludes = new ArrayList<>();
        for (TableFilterRule rule : rules) {
            if (rule.getRuleType() == TableFilterRule.RuleType.INCLUDE) {
                includes.add(rule);
            } else {
                excludes.add(rule);
            }
        }
        this.includeRules = Collections.unmodifiableList(includes);
        this.excludeRules = Collections.unmodifiableList(excludes);
    }

    /**
     * Determine whether a table should be warmed up.
     * 1. If the table matches any INCLUDE rule → candidate
     * 2. If the candidate matches any EXCLUDE rule → excluded
     */
    public boolean shouldWarmUp(String dbName, String tableName) {
        String fullName = dbName + "." + tableName;

        boolean included = includeRules.stream()
                .anyMatch(rule -> rule.matches(fullName));
        if (!included) {
            return false;
        }

        boolean excluded = excludeRules.stream()
                .anyMatch(rule -> rule.matches(fullName));
        return !excluded;
    }

    public List<TableFilterRule> getIncludeRules() {
        return includeRules;
    }

    public List<TableFilterRule> getExcludeRules() {
        return excludeRules;
    }

    /**
     * Get all rules (include + exclude) for iteration.
     */
    public List<TableFilterRule> getAllRules() {
        List<TableFilterRule> all = new ArrayList<>(includeRules.size() + excludeRules.size());
        all.addAll(includeRules);
        all.addAll(excludeRules);
        return all;
    }

    /**
     * Generate a human-readable string representation for logging.
     */
    @Override
    public String toString() {
        return "OnTablesFilter{include=" + includeRules.stream()
                .map(TableFilterRule::getRawPattern)
                .collect(Collectors.joining(", "))
                + ", exclude=" + excludeRules.stream()
                .map(TableFilterRule::getRawPattern)
                .collect(Collectors.joining(", ")) + "}";
    }
}
