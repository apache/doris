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
 *
 * <p>Design principles:
 * <ul>
 *   <li>FE transmits user intent clearly to BE</li>
 *   <li>Empty string = user did not specify analyzer (BE chooses)</li>
 *   <li>Non-empty string = user explicitly specified analyzer (BE exact matches)</li>
 * </ul>
 *
 * <p>This simple protocol eliminates ambiguity between "no preference" and "none" analyzer.
 */
public final class AnalyzerSelector {

    private AnalyzerSelector() {
    }

    /**
     * Select analyzer configuration based on index and user request.
     *
     * @param index            The inverted index (may be null if table has no index)
     * @param requestedAnalyzer User-specified analyzer from USING ANALYZER clause (may be null)
     * @return Selection containing analyzer and parser information
     */
    public static Selection select(Index index, String requestedAnalyzer) {
        Map<String, String> properties = index == null ? Collections.emptyMap() : index.getProperties();
        return select(properties, requestedAnalyzer, index == null);
    }

    /**
     * Select analyzer configuration from properties map.
     * Used when Index object is not available.
     */
    public static Selection select(Map<String, String> properties, String requestedAnalyzer) {
        return select(properties, requestedAnalyzer, properties == null);
    }

    private static Selection select(Map<String, String> properties, String requestedAnalyzer, boolean noIndex) {
        if (properties == null) {
            properties = Collections.emptyMap();
        }

        // 1. Normalize user request
        String userAnalyzer = normalize(requestedAnalyzer);

        // 2. Get index configuration
        String indexParser = InvertedIndexUtil.getInvertedIndexParser(properties);
        String indexCustomAnalyzer = InvertedIndexUtil.getPreferredAnalyzer(properties);

        // 3. Determine parser for slow path tokenization
        //    - No index: use english (more aggressive tokenization)
        //    - Has index with explicit parser: use index's configured parser
        //    - Has index with custom analyzer (no explicit parser): use english
        //      (custom analyzer implies fulltext tokenization)
        //    - Has index but no analyzer/parser: use none (keyword matching)
        String parser;
        if (noIndex) {
            parser = InvertedIndexUtil.INVERTED_INDEX_PARSER_ENGLISH;
        } else if (!Strings.isNullOrEmpty(indexParser)
                && !InvertedIndexUtil.INVERTED_INDEX_PARSER_NONE.equalsIgnoreCase(indexParser)) {
            parser = indexParser;
        } else if (!Strings.isNullOrEmpty(indexCustomAnalyzer)) {
            // Custom analyzer exists but no explicit parser
            // Use english for slow-path tokenization since custom analyzer implies fulltext
            parser = InvertedIndexUtil.INVERTED_INDEX_PARSER_ENGLISH;
        } else {
            parser = InvertedIndexUtil.INVERTED_INDEX_PARSER_NONE;
        }

        // 4. Create selection
        //    - userAnalyzer: what user explicitly specified (empty if not specified)
        //    - indexAnalyzer: what the index is configured with (for slow path fallback)
        String indexAnalyzer = determineIndexAnalyzer(indexCustomAnalyzer, indexParser);

        return new Selection(userAnalyzer, indexAnalyzer, parser, !Strings.isNullOrEmpty(userAnalyzer));
    }

    /**
     * Determine the analyzer configured on the index.
     * Priority: custom analyzer > builtin parser (if not "none")
     */
    private static String determineIndexAnalyzer(String customAnalyzer, String parser) {
        if (!Strings.isNullOrEmpty(customAnalyzer)) {
            return customAnalyzer.trim().toLowerCase();
        }
        if (!Strings.isNullOrEmpty(parser)
                && !InvertedIndexUtil.INVERTED_INDEX_PARSER_NONE.equalsIgnoreCase(parser)) {
            return parser.trim().toLowerCase();
        }
        // Keyword index (parser=none) or no analyzer configured
        return "";
    }

    private static String normalize(String analyzer) {
        return analyzer == null ? "" : analyzer.trim().toLowerCase();
    }

    /**
     * Result of analyzer selection.
     *
     * <p>Key semantics:
     * <ul>
     *   <li>{@link #effectiveAnalyzerName()}: What to send to BE for index selection</li>
     *   <li>{@link #parser()}: Parser type for slow path tokenization</li>
     * </ul>
     */
    public static final class Selection {
        // User explicitly specified analyzer (empty if not specified)
        private final String userAnalyzer;
        // Analyzer configured on index (empty for keyword index)
        private final String indexAnalyzer;
        // Parser type for slow path
        private final String parser;
        // Whether user explicitly specified an analyzer
        private final boolean explicit;

        private Selection(String userAnalyzer, String indexAnalyzer, String parser, boolean explicit) {
            this.userAnalyzer = userAnalyzer;
            this.indexAnalyzer = indexAnalyzer;
            this.parser = Strings.isNullOrEmpty(parser)
                    ? InvertedIndexUtil.INVERTED_INDEX_PARSER_NONE
                    : parser.trim().toLowerCase();
            this.explicit = explicit;
        }

        /**
         * Get the analyzer to use. For backwards compatibility, returns either
         * the user-specified analyzer or the index-configured analyzer.
         */
        public String analyzer() {
            return !Strings.isNullOrEmpty(userAnalyzer) ? userAnalyzer : indexAnalyzer;
        }

        /**
         * Get the parser type for slow path tokenization.
         */
        public String parser() {
            return parser;
        }

        /**
         * Whether user explicitly specified an analyzer via USING ANALYZER clause.
         */
        public boolean explicit() {
            return explicit;
        }

        /**
         * Compute the analyzer name to send to BE.
         *
         * <p>Returns the analyzer that should be used for query tokenization:
         * <ul>
         *   <li>User specified analyzer → use that analyzer</li>
         *   <li>User did not specify → use index's configured analyzer</li>
         * </ul>
         *
         * <p>This ensures the query uses the same analyzer as the index for consistent
         * tokenization. For custom analyzers, this is critical because the query needs
         * to use the same custom analyzer that was used for indexing.
         *
         * @param hasIndex      Whether the table has an inverted index
         * @param fallbackParser Unused, kept for API compatibility
         * @return Analyzer name for BE
         */
        public String effectiveAnalyzerName(boolean hasIndex, String fallbackParser) {
            // Return userAnalyzer if specified, otherwise indexAnalyzer
            // This ensures custom analyzers work correctly - the query must use
            // the same analyzer as the index for proper tokenization matching.
            return analyzer();
        }
    }
}
