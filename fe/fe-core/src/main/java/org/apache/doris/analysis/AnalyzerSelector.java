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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Index;

import com.google.common.base.Strings;

import java.util.Collections;
import java.util.Map;

/**
 * Helper for selecting the analyzer that should be attached to a MATCH predicate.
 * The current implementation is intentionally simple so that future rule-based
 * selection can plug in without touching the planner surface again.
 */
public final class AnalyzerSelector {

    private AnalyzerSelector() {
    }

    public static Selection select(Index index, String requestedAnalyzer) {
        Map<String, String> properties = index == null ? Collections.emptyMap() : index.getProperties();
        return select(properties, requestedAnalyzer);
    }

    public static Selection select(Map<String, String> properties, String requestedAnalyzer) {
        String normalizedRequest = normalize(requestedAnalyzer);
        String preferredAnalyzer = selectDefaultAnalyzer(properties);
        String parser = InvertedIndexUtil.getInvertedIndexParser(properties);

        if (!Strings.isNullOrEmpty(normalizedRequest)) {
            return new Selection(normalizedRequest, parser, true);
        }

        String resolvedAnalyzer;
        if (!Strings.isNullOrEmpty(preferredAnalyzer)) {
            resolvedAnalyzer = preferredAnalyzer;
        } else if (!Strings.isNullOrEmpty(parser)
                && !InvertedIndexUtil.INVERTED_INDEX_PARSER_NONE.equalsIgnoreCase(parser)) {
            resolvedAnalyzer = parser;
        } else {
            resolvedAnalyzer = "";
        }
        return new Selection(resolvedAnalyzer, parser, false);
    }

    private static String normalize(String analyzer) {
        // Policy names are case-insensitive, convert to lowercase for consistent lookup
        return analyzer == null ? "" : analyzer.trim().toLowerCase();
    }

    private static String selectDefaultAnalyzer(Map<String, String> properties) {
        // Placeholder for future rule-based selection. At the moment we simply
        // reuse the analyzer attached to the index, if any.
        return InvertedIndexUtil.getPreferredAnalyzer(properties);
    }

    public static final class Selection {
        private final String analyzer;
        private final String parser;
        private final boolean explicit;

        private Selection(String analyzer, String parser, boolean explicit) {
            this.analyzer = normalize(analyzer);
            String normalizedParser = normalize(parser);
            this.parser = Strings.isNullOrEmpty(normalizedParser)
                    ? InvertedIndexUtil.INVERTED_INDEX_PARSER_NONE
                    : normalizedParser;
            this.explicit = explicit;
        }

        public String analyzer() {
            return analyzer;
        }

        public String parser() {
            return parser;
        }

        public boolean explicit() {
            return explicit;
        }

        /**
         * Computes the effective analyzer name for BE execution.
         * - If no index exists and analyzer wasn't explicitly specified: use "__default__"
         * - Otherwise: use the resolved analyzer, falling back to parser if empty
         */
        public String effectiveAnalyzerName(boolean hasIndex, String fallbackParser) {
            if (!hasIndex && !explicit) {
                return "__default__";
            }
            return Strings.isNullOrEmpty(analyzer) ? fallbackParser : analyzer;
        }
    }
}

