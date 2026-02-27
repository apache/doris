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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.search.SearchLexer;
import org.apache.doris.nereids.search.SearchParser;
import org.apache.doris.nereids.search.SearchParserBaseVisitor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Search DSL Parser using ANTLR-generated parser.
 * Parses DSL syntax and extracts field bindings for FE->BE communication.
 * <p>
 * Supported syntax:
 * - field:term
 * - field:"quoted term"
 * - field:prefix*
 * - field:*wildcard*
 * - field:/regexp/
 * - AND/OR/NOT operators
 * - Parentheses for grouping
 * - Range queries: field:[1 TO 10], field:{1 TO 10}
 * - List queries: field:IN(value1 value2)
 * - Any/All queries: field:ANY(value), field:ALL(value)
 */
public class SearchDslParser {
    private static final Logger LOG = LogManager.getLogger(SearchDslParser.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int MAX_FIELD_GROUP_DEPTH = 32;

    /**
     * Exception for search DSL syntax errors.
     * This exception is thrown when the DSL string cannot be parsed due to syntax issues.
     * It is distinct from programming errors (NullPointerException, etc.) to provide
     * clearer error messages to users.
     */
    public static class SearchDslSyntaxException extends RuntimeException {
        public SearchDslSyntaxException(String message) {
            super(message);
        }

        public SearchDslSyntaxException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Parse DSL string and return intermediate representation
     */
    public static QsPlan parseDsl(String dsl) {
        return parseDsl(dsl, (String) null);
    }

    /**
     * Parse DSL string with JSON options support.
     * This is the primary method for the new 2-parameter search function signature.
     *
     * @param dsl DSL query string
     * @param optionsJson JSON options string containing all configuration:
     *                    - default_field: default field name when DSL doesn't specify field
     *                    - default_operator: "and" or "or" for multi-term queries
     *                    - mode: "standard" or "lucene"
     *                    - minimum_should_match: integer for Lucene mode
     *                    - fields: array of field names for multi-field search
     *                    - type: "best_fields" (default) or "cross_fields" for multi-field semantics
     *                    Example: '{"default_field":"title","mode":"lucene","minimum_should_match":0}'
     *                    Example: '{"fields":["title","content"],"default_operator":"and"}'
     *                    Example: '{"fields":["title","content"],"type":"cross_fields"}'
     * @return Parsed QsPlan
     */
    public static QsPlan parseDsl(String dsl, @Nullable String optionsJson) {
        // Parse options from JSON
        SearchOptions searchOptions = parseOptions(optionsJson);

        // Extract default_field and default_operator from options
        String defaultField = searchOptions.getDefaultField();
        String defaultOperator = searchOptions.getDefaultOperator();

        QsPlan plan;
        // Use Lucene mode parser if specified
        if (searchOptions.isLuceneMode()) {
            // Multi-field + Lucene mode: first expand DSL, then parse with Lucene semantics
            if (searchOptions.isMultiFieldMode()) {
                plan = parseDslMultiFieldLuceneMode(dsl, searchOptions.getFields(),
                        defaultOperator, searchOptions);
            } else {
                plan = parseDslLuceneMode(dsl, defaultField, defaultOperator, searchOptions);
            }
        } else if (searchOptions.isMultiFieldMode()) {
            // Multi-field mode parsing (standard mode)
            plan = parseDslMultiFieldMode(dsl, searchOptions.getFields(), defaultOperator, searchOptions);
        } else {
            // Standard mode parsing
            plan = parseDslStandardMode(dsl, defaultField, defaultOperator);
        }

        // Wrap plan with options for BE serialization
        // NOTE: Must use normalizeDefaultOperator() here because BE compares
        // default_operator case-sensitively against lowercase "and"/"or"
        return new QsPlan(plan.getRoot(), plan.getFieldBindings(),
                normalizeDefaultOperator(searchOptions.getDefaultOperator()),
                searchOptions.getMinimumShouldMatch());
    }

    /**
     * Parse DSL string with default field and operator support (legacy method).
     * Kept for backward compatibility.
     *
     * @param dsl DSL query string
     * @param defaultField Default field name when DSL doesn't specify field (optional)
     * @param defaultOperator Default operator ("and" or "or") for multi-term queries (optional, defaults to "or")
     * @return Parsed QsPlan
     */
    public static QsPlan parseDsl(String dsl, @Nullable String defaultField, @Nullable String defaultOperator) {
        return parseDslStandardMode(dsl, defaultField, defaultOperator);
    }

    /**
     * Standard mode parsing (original behavior)
     * Now uses single-phase parsing: ANTLR grammar natively supports bare queries (without field prefix),
     * and the visitor fills in the default field.
     */
    private static QsPlan parseDslStandardMode(String dsl, String defaultField, String defaultOperator) {
        if (dsl == null || dsl.trim().isEmpty()) {
            return new QsPlan(new QsNode(QsClauseType.TERM, "error", "empty_dsl"), new ArrayList<>());
        }

        // Parse original DSL directly - no preprocessing needed
        // The ANTLR grammar now supports bareQuery (without field prefix)
        // and QsAstBuilder will use defaultField for bare queries
        String trimmedDsl = dsl.trim();

        try {
            // Create ANTLR lexer and parser
            SearchLexer lexer = new SearchLexer(CharStreams.fromString(trimmedDsl));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SearchParser parser = new SearchParser(tokens);

            // Add error listener to detect parsing errors
            parser.removeErrorListeners();
            parser.addErrorListener(new org.antlr.v4.runtime.BaseErrorListener() {
                @Override
                public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                        Object offendingSymbol,
                        int line, int charPositionInLine,
                        String msg, org.antlr.v4.runtime.RecognitionException e) {
                    throw new SearchDslSyntaxException("Syntax error at line " + line
                            + ":" + charPositionInLine + " " + msg);
                }
            });

            // Parse the search query
            ParseTree tree = parser.search();

            // Check if parsing was successful
            if (tree == null) {
                throw new SearchDslSyntaxException("Invalid search DSL syntax: parsing returned null");
            }

            // Build AST using visitor pattern with defaultField and defaultOperator for bare queries
            QsAstBuilder visitor = new QsAstBuilder(defaultField, defaultOperator);
            QsNode root = visitor.visit(tree);

            // Extract field bindings
            Set<String> fieldNames = visitor.getFieldNames();
            List<QsFieldBinding> bindings = new ArrayList<>();
            int slotIndex = 0;
            for (String fieldName : fieldNames) {
                bindings.add(new QsFieldBinding(fieldName, slotIndex++));
            }

            return new QsPlan(root, bindings);

        } catch (SearchDslSyntaxException e) {
            // Syntax error in DSL - user input issue
            LOG.error("Failed to parse search DSL: '{}' (defaultField={}, defaultOperator={})",
                    dsl, defaultField, defaultOperator, e);
            throw new SearchDslSyntaxException("Invalid search DSL: " + dsl + ". " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            // Invalid argument - user input issue
            LOG.error("Invalid argument in search DSL: '{}' (defaultField={}, defaultOperator={})",
                    dsl, defaultField, defaultOperator, e);
            throw new IllegalArgumentException("Invalid search DSL argument: " + dsl + ". " + e.getMessage(), e);
        } catch (NullPointerException e) {
            // Internal error - programming bug
            LOG.error("Internal error (NPE) while parsing search DSL: '{}' (defaultField={}, defaultOperator={})",
                    dsl, defaultField, defaultOperator, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + dsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (IndexOutOfBoundsException e) {
            // Internal error - programming bug
            LOG.error("Internal error (IOOB) while parsing search DSL: '{}' (defaultField={}, defaultOperator={})",
                    dsl, defaultField, defaultOperator, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + dsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            // Other runtime errors
            LOG.error("Unexpected error while parsing search DSL: '{}' (defaultField={}, defaultOperator={})",
                    dsl, defaultField, defaultOperator, e);
            throw new RuntimeException("Unexpected error parsing search DSL: " + dsl + ". " + e.getMessage(), e);
        }
    }

    /**
     * Normalize default operator to lowercase "and" or "or"
     */
    private static String normalizeDefaultOperator(String operator) {
        if (operator == null || operator.trim().isEmpty()) {
            return "or";  // Default to OR
        }
        String normalized = operator.trim().toLowerCase();
        if ("and".equals(normalized) || "or".equals(normalized)) {
            return normalized;
        }
        throw new IllegalArgumentException("Invalid default operator: " + operator
                + ". Must be 'and' or 'or'");
    }

    // ============ Common Helper Methods ============

    /**
     * Create an error QsPlan for empty DSL input.
     */
    private static QsPlan createEmptyDslErrorPlan() {
        return new QsPlan(new QsNode(QsClauseType.TERM, "error", "empty_dsl"), new ArrayList<>());
    }

    /**
     * Validate that DSL is not null or empty.
     * @return true if DSL is valid (non-null, non-empty)
     */
    private static boolean isValidDsl(String dsl) {
        return dsl != null && !dsl.trim().isEmpty();
    }

    /**
     * Validate fields list for multi-field mode.
     * @throws IllegalArgumentException if fields is null or empty
     */
    private static void validateFieldsList(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException(
                    "fields list cannot be null or empty for multi-field mode, got: " + fields);
        }
    }

    /**
     * Collect all field names from an AST node recursively.
     * @param node The AST node to collect from
     * @return Set of field names
     */
    private static Set<String> collectFieldNames(QsNode node) {
        Set<String> fieldNames = new LinkedHashSet<>();
        collectFieldNamesRecursive(node, fieldNames);
        return fieldNames;
    }

    private static void collectFieldNamesRecursive(QsNode node, Set<String> fieldNames) {
        if (node == null) {
            return;
        }
        // Add field name if it's a leaf node with a field
        if (node.getField() != null && !node.getField().isEmpty()) {
            fieldNames.add(node.getField());
        }
        // Recursively collect from children
        if (node.getChildren() != null) {
            for (QsNode child : node.getChildren()) {
                collectFieldNamesRecursive(child, fieldNames);
            }
        }
    }

    /**
     * Recursively mark leaf nodes with the given field name and set explicitField=true.
     * Used for field-grouped queries like title:(rock OR jazz) to ensure inner leaf nodes
     * are bound to the group's field and are not re-expanded by MultiFieldExpander.
     *
     * Skips nodes already marked as explicitField to preserve inner explicit bindings,
     * e.g., title:(content:foo OR bar) keeps content:foo intact and only sets title on bar.
     *
     * @param depth current recursion depth to prevent StackOverflow from malicious input
     */
    private static void markExplicitFieldRecursive(QsNode node, String field) {
        markExplicitFieldRecursive(node, field, 0);
    }

    private static void markExplicitFieldRecursive(QsNode node, String field, int depth) {
        if (node == null) {
            return;
        }
        if (depth > MAX_FIELD_GROUP_DEPTH) {
            throw new SearchDslSyntaxException(
                    "Field group query nesting too deep (max " + MAX_FIELD_GROUP_DEPTH + ")");
        }
        // Skip nodes already explicitly bound to a field (e.g., inner field:term inside a group)
        if (node.isExplicitField()) {
            return;
        }
        if (node.getChildren() != null && !node.getChildren().isEmpty()) {
            for (QsNode child : node.getChildren()) {
                markExplicitFieldRecursive(child, field, depth + 1);
            }
        } else {
            // Leaf node - set field and mark as explicit
            node.setField(field);
            node.setExplicitField(true);
        }
    }

    /**
     * Common ANTLR parsing helper with visitor pattern.
     * Reduces code duplication across parsing methods.
     *
     * @param expandedDsl The expanded DSL string to parse
     * @param visitorFactory Factory function to create the appropriate visitor
     * @param originalDsl Original DSL for error messages
     * @param modeDescription Description of the parsing mode for error messages
     * @return Parsed QsPlan
     */
    private static QsPlan parseWithVisitor(String expandedDsl,
            Function<SearchParser, FieldTrackingVisitor> visitorFactory,
            String originalDsl, String modeDescription) {
        try {
            // Create ANTLR lexer and parser
            SearchLexer lexer = new SearchLexer(CharStreams.fromString(expandedDsl));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SearchParser parser = new SearchParser(tokens);

            // Add error listener
            parser.removeErrorListeners();
            parser.addErrorListener(new org.antlr.v4.runtime.BaseErrorListener() {
                @Override
                public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                        Object offendingSymbol,
                        int line, int charPositionInLine,
                        String msg, org.antlr.v4.runtime.RecognitionException e) {
                    throw new SearchDslSyntaxException("Syntax error at line " + line
                            + ":" + charPositionInLine + " " + msg);
                }
            });

            ParseTree tree = parser.search();
            if (tree == null) {
                throw new SearchDslSyntaxException("Invalid search DSL syntax: parsing returned null");
            }

            // Build AST using provided visitor
            FieldTrackingVisitor visitor = visitorFactory.apply(parser);
            QsNode root = visitor.visit(tree);

            // Extract field bindings
            Set<String> fieldNames = visitor.getFieldNames();
            List<QsFieldBinding> bindings = new ArrayList<>();
            int slotIndex = 0;
            for (String fieldName : fieldNames) {
                bindings.add(new QsFieldBinding(fieldName, slotIndex++));
            }

            return new QsPlan(root, bindings);

        } catch (SearchDslSyntaxException e) {
            // Syntax error in DSL - user input issue
            LOG.error("Failed to parse search DSL in {}: '{}' (expanded: '{}')",
                    modeDescription, originalDsl, expandedDsl, e);
            throw new SearchDslSyntaxException("Invalid search DSL: " + originalDsl + ". " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            // Invalid argument - user input issue
            LOG.error("Invalid argument in search DSL ({}): '{}' (expanded: '{}')",
                    modeDescription, originalDsl, expandedDsl, e);
            throw new IllegalArgumentException("Invalid search DSL argument: " + originalDsl
                    + ". " + e.getMessage(), e);
        } catch (NullPointerException e) {
            // Internal error - programming bug
            LOG.error("Internal error (NPE) while parsing search DSL in {}: '{}' (expanded: '{}')",
                    modeDescription, originalDsl, expandedDsl, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + originalDsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (IndexOutOfBoundsException e) {
            // Internal error - programming bug
            LOG.error("Internal error (IOOB) while parsing search DSL in {}: '{}' (expanded: '{}')",
                    modeDescription, originalDsl, expandedDsl, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + originalDsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            // Other runtime errors
            LOG.error("Unexpected error while parsing search DSL in {}: '{}' (expanded: '{}')",
                    modeDescription, originalDsl, expandedDsl, e);
            throw new RuntimeException("Unexpected error parsing search DSL: " + originalDsl
                    + ". " + e.getMessage(), e);
        }
    }

    /**
     * Parse DSL in multi-field mode.
     * Expansion behavior depends on the type option:
     * - best_fields (default): all terms must match within the same field
     * - cross_fields: terms can match across different fields
     *
     * @param dsl DSL query string
     * @param fields List of field names to search
     * @param defaultOperator "and" or "or" for joining term groups
     * @param options Search options containing type setting
     * @return Parsed QsPlan
     */
    private static QsPlan parseDslMultiFieldMode(String dsl, List<String> fields, String defaultOperator,
            SearchOptions options) {
        if (!isValidDsl(dsl)) {
            return createEmptyDslErrorPlan();
        }
        validateFieldsList(fields);

        String trimmedDsl = dsl.trim();

        try {
            // Parse original DSL directly using first field as placeholder for bare queries
            // The AST will be expanded to all fields in post-processing
            SearchLexer lexer = new SearchLexer(CharStreams.fromString(trimmedDsl));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SearchParser parser = new SearchParser(tokens);

            parser.removeErrorListeners();
            parser.addErrorListener(new org.antlr.v4.runtime.BaseErrorListener() {
                @Override
                public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                        Object offendingSymbol,
                        int line, int charPositionInLine,
                        String msg, org.antlr.v4.runtime.RecognitionException e) {
                    throw new SearchDslSyntaxException("Syntax error at line " + line
                            + ":" + charPositionInLine + " " + msg);
                }
            });

            ParseTree tree = parser.search();
            if (tree == null) {
                throw new SearchDslSyntaxException("Invalid search DSL syntax: parsing returned null");
            }

            // Build AST using first field as placeholder for bare queries, with default operator
            QsAstBuilder visitor = new QsAstBuilder(fields.get(0), defaultOperator);
            QsNode root = visitor.visit(tree);

            // Apply multi-field expansion based on type
            QsNode expandedRoot;
            if (options.isCrossFieldsMode()) {
                // cross_fields: each term expands to (field1:term OR field2:term)
                expandedRoot = MultiFieldExpander.expandCrossFields(root, fields);
            } else if (options.isBestFieldsMode()) {
                // best_fields: entire query copied per field, joined with OR
                expandedRoot = MultiFieldExpander.expandBestFields(root, fields);
            } else {
                throw new IllegalStateException(
                        "Invalid type value: '" + options.getType() + "'. Expected 'best_fields' or 'cross_fields'");
            }

            // Extract field bindings from expanded AST
            Set<String> fieldNames = collectFieldNames(expandedRoot);
            // If no fields were collected (e.g., MATCH_ALL_DOCS query that matches all docs
            // regardless of field), use the original fields list to ensure proper push-down
            if (fieldNames.isEmpty()) {
                fieldNames = new LinkedHashSet<>(fields);
            }
            List<QsFieldBinding> bindings = new ArrayList<>();
            int slotIndex = 0;
            for (String fieldName : fieldNames) {
                bindings.add(new QsFieldBinding(fieldName, slotIndex++));
            }

            return new QsPlan(expandedRoot, bindings);

        } catch (SearchDslSyntaxException e) {
            LOG.error("Failed to parse search DSL in multi-field mode: '{}'", dsl, e);
            throw new SearchDslSyntaxException("Invalid search DSL: " + dsl + ". " + e.getMessage(), e);
        } catch (RuntimeException e) {
            LOG.error("Unexpected error while parsing search DSL in multi-field mode: '{}'", dsl, e);
            throw new RuntimeException("Unexpected error parsing search DSL: " + dsl + ". " + e.getMessage(), e);
        }
    }

    /**
     * Parse DSL in multi-field mode with Lucene boolean semantics.
     * First expands DSL across fields, then applies Lucene-style MUST/SHOULD/MUST_NOT logic.
     * Expansion behavior depends on the type option (best_fields or cross_fields).
     *
     * @param dsl DSL query string
     * @param fields List of field names to search
     * @param defaultOperator "and" or "or" for joining term groups
     * @param options Search options containing Lucene mode settings and type
     * @return Parsed QsPlan with Lucene boolean semantics
     */
    private static QsPlan parseDslMultiFieldLuceneMode(String dsl, List<String> fields,
            String defaultOperator, SearchOptions options) {
        if (!isValidDsl(dsl)) {
            return createEmptyDslErrorPlan();
        }
        validateFieldsList(fields);

        // For multi-field mode (fields.size() > 1), ignore minimum_should_match.
        // The expanded DSL creates complex nested boolean structures where msm
        // semantics become ambiguous. This is a deliberate design decision.
        final SearchOptions effectiveOptions = fields.size() > 1
                ? options.withMinimumShouldMatch(null) : options;

        String trimmedDsl = dsl.trim();

        try {
            // Parse original DSL directly using first field as placeholder for bare queries
            SearchLexer lexer = new SearchLexer(CharStreams.fromString(trimmedDsl));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SearchParser parser = new SearchParser(tokens);

            parser.removeErrorListeners();
            parser.addErrorListener(new org.antlr.v4.runtime.BaseErrorListener() {
                @Override
                public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                        Object offendingSymbol,
                        int line, int charPositionInLine,
                        String msg, org.antlr.v4.runtime.RecognitionException e) {
                    throw new SearchDslSyntaxException("Syntax error at line " + line
                            + ":" + charPositionInLine + " " + msg);
                }
            });

            ParseTree tree = parser.search();
            if (tree == null) {
                throw new SearchDslSyntaxException("Invalid search DSL syntax: parsing returned null");
            }

            // Build AST using Lucene-mode visitor with first field as placeholder for bare queries
            // Use constructor with override to avoid mutating shared options object (thread-safety)
            QsLuceneModeAstBuilder visitor = new QsLuceneModeAstBuilder(effectiveOptions, fields.get(0));
            QsNode root = visitor.visit(tree);

            // In ES query_string, both best_fields and cross_fields use per-clause expansion
            // (each clause is independently expanded across fields). The difference is only
            // in scoring (dis_max vs blended analysis), which doesn't apply to Doris since
            // search() is a boolean filter. So we always use expandCrossFields here.
            // Type validation already happened in SearchOptions.setType().
            QsNode expandedRoot = MultiFieldExpander.expandCrossFields(root, fields, true);

            // Extract field bindings from expanded AST
            Set<String> fieldNames = collectFieldNames(expandedRoot);
            // If no fields were collected (e.g., MATCH_ALL_DOCS query that matches all docs
            // regardless of field), use the original fields list to ensure proper push-down
            if (fieldNames.isEmpty()) {
                fieldNames = new LinkedHashSet<>(fields);
            }
            List<QsFieldBinding> bindings = new ArrayList<>();
            int slotIndex = 0;
            for (String fieldName : fieldNames) {
                bindings.add(new QsFieldBinding(fieldName, slotIndex++));
            }

            // Include default_operator and minimum_should_match for BE
            return new QsPlan(expandedRoot, bindings,
                    normalizeDefaultOperator(effectiveOptions.getDefaultOperator()),
                    effectiveOptions.getMinimumShouldMatch());

        } catch (SearchDslSyntaxException e) {
            LOG.error("Failed to parse search DSL in multi-field Lucene mode: '{}'", dsl, e);
            throw new SearchDslSyntaxException("Invalid search DSL: " + dsl + ". " + e.getMessage(), e);
        } catch (RuntimeException e) {
            LOG.error("Unexpected error while parsing search DSL in multi-field Lucene mode: '{}'", dsl, e);
            throw new RuntimeException("Unexpected error parsing search DSL: " + dsl + ". " + e.getMessage(), e);
        }
    }

    /**
     * Clause types supported
     */
    public enum QsClauseType {
        TERM,           // field:value
        PHRASE,         // field:"phrase search"
        PREFIX,         // field:prefix*
        WILDCARD,       // field:*wild*card*
        REGEXP,         // field:/pattern/
        RANGE,          // field:[1 TO 10] or field:{1 TO 10}
        LIST,           // field:IN(value1 value2)
        ANY,            // field:ANY(value) - any match
        ALL,            // field:ALL(value) - all match
        EXACT,          // field:EXACT(value) - exact match without tokenization
        AND,            // clause1 AND clause2 (standard boolean algebra)
        OR,             // clause1 OR clause2 (standard boolean algebra)
        NOT,            // NOT clause (standard boolean algebra)
        OCCUR_BOOLEAN,  // Lucene-style boolean query with MUST/SHOULD/MUST_NOT
        MATCH_ALL_DOCS  // Matches all documents (used for pure NOT query rewriting)
    }

    /**
     * Occur type for Lucene-style boolean queries
     */
    public enum QsOccur {
        MUST,       // Term must appear (equivalent to +term)
        SHOULD,     // Term should appear (optional)
        MUST_NOT    // Term must not appear (equivalent to -term)
    }

    /**
     * Common interface for AST builders that track field names.
     * Both QsAstBuilder and QsLuceneModeAstBuilder implement this interface.
     */
    private interface FieldTrackingVisitor {
        Set<String> getFieldNames();

        QsNode visit(ParseTree tree);
    }

    /**
     * ANTLR visitor to build QsNode AST from parse tree
     */
    private static class QsAstBuilder extends SearchParserBaseVisitor<QsNode> implements FieldTrackingVisitor {
        private final Set<String> fieldNames = new LinkedHashSet<>();
        // Context stack to track current field name during parsing
        private String currentFieldName = null;
        // Default field for bare queries (without field: prefix)
        private final String defaultField;
        // Default operator for implicit conjunction (space-separated terms): "AND" or "OR"
        private final String defaultOperator;

        /**
         * Creates a QsAstBuilder with no default field.
         * Bare queries will throw an error.
         */
        public QsAstBuilder() {
            this.defaultField = null;
            this.defaultOperator = "OR";
        }

        /**
         * Creates a QsAstBuilder with a default field for bare queries.
         * @param defaultField The field to use for queries without explicit field prefix
         */
        public QsAstBuilder(String defaultField) {
            this.defaultField = defaultField;
            this.defaultOperator = "OR";
        }

        /**
         * Creates a QsAstBuilder with default field and default operator.
         * @param defaultField The field to use for queries without explicit field prefix
         * @param defaultOperator The operator to use for implicit conjunction ("AND" or "OR")
         * @throws IllegalArgumentException if defaultOperator is not null and not "and" or "or"
         */
        public QsAstBuilder(String defaultField, String defaultOperator) {
            this.defaultField = defaultField;
            // Validate default operator
            if (defaultOperator != null && !defaultOperator.trim().isEmpty()) {
                String normalized = defaultOperator.trim().toUpperCase();
                if (!"AND".equals(normalized) && !"OR".equals(normalized)) {
                    throw new IllegalArgumentException(
                            "Invalid default operator: '" + defaultOperator
                            + "'. Must be 'and' or 'or'");
                }
                this.defaultOperator = normalized;
            } else {
                this.defaultOperator = "OR";  // Default to OR
            }
        }

        public Set<String> getFieldNames() {
            return Collections.unmodifiableSet(fieldNames);
        }

        @Override
        public QsNode visitSearch(SearchParser.SearchContext ctx) {
            QsNode result = visit(ctx.clause());
            if (result == null) {
                throw new RuntimeException("Invalid search clause");
            }
            return result;
        }

        @Override
        public QsNode visitOrClause(SearchParser.OrClauseContext ctx) {
            if (ctx.andClause().size() == 1) {
                QsNode result = visit(ctx.andClause(0));
                if (result == null) {
                    throw new RuntimeException("Invalid OR clause operand");
                }
                return result;
            }

            List<QsNode> children = new ArrayList<>();
            for (SearchParser.AndClauseContext andCtx : ctx.andClause()) {
                QsNode child = visit(andCtx);
                if (child == null) {
                    throw new RuntimeException("Invalid OR clause operand");
                }
                children.add(child);
            }
            return new QsNode(QsClauseType.OR, children);
        }

        @Override
        public QsNode visitAndClause(SearchParser.AndClauseContext ctx) {
            if (ctx.notClause().size() == 1) {
                QsNode result = visit(ctx.notClause(0));
                if (result == null) {
                    throw new RuntimeException("Invalid AND clause operand");
                }
                return result;
            }

            List<QsNode> children = new ArrayList<>();
            for (SearchParser.NotClauseContext notCtx : ctx.notClause()) {
                QsNode child = visit(notCtx);
                if (child == null) {
                    throw new RuntimeException("Invalid AND clause operand");
                }
                children.add(child);
            }

            // Check if there are explicit AND tokens
            // If no explicit AND tokens, use the default operator for implicit conjunction
            List<org.antlr.v4.runtime.tree.TerminalNode> andTokens = ctx.AND();
            boolean hasExplicitAnd = andTokens != null && !andTokens.isEmpty();

            QsClauseType clauseType;
            if (hasExplicitAnd) {
                // Explicit AND - always use AND
                clauseType = QsClauseType.AND;
            } else {
                // Implicit conjunction - use default operator
                clauseType = "AND".equalsIgnoreCase(defaultOperator) ? QsClauseType.AND : QsClauseType.OR;
            }

            return new QsNode(clauseType, children);
        }

        @Override
        public QsNode visitNotClause(SearchParser.NotClauseContext ctx) {
            if (ctx.NOT() != null) {
                QsNode child = visit(ctx.atomClause());
                if (child == null) {
                    throw new RuntimeException("Invalid NOT clause: missing operand");
                }
                List<QsNode> children = new ArrayList<>();
                children.add(child);
                return new QsNode(QsClauseType.NOT, children);
            }
            QsNode result = visit(ctx.atomClause());
            if (result == null) {
                throw new RuntimeException("Invalid atom clause");
            }
            return result;
        }

        @Override
        public QsNode visitAtomClause(SearchParser.AtomClauseContext ctx) {
            if (ctx.clause() != null) {
                // Parenthesized clause
                QsNode result = visit(ctx.clause());
                if (result == null) {
                    throw new RuntimeException("Invalid parenthesized clause");
                }
                return result;
            }
            if (ctx.fieldGroupQuery() != null) {
                QsNode result = visit(ctx.fieldGroupQuery());
                if (result == null) {
                    throw new SearchDslSyntaxException("Invalid field group query");
                }
                return result;
            }
            if (ctx.fieldQuery() != null) {
                QsNode result = visit(ctx.fieldQuery());
                if (result == null) {
                    throw new RuntimeException("Invalid field query");
                }
                return result;
            }
            if (ctx.bareQuery() != null) {
                QsNode result = visit(ctx.bareQuery());
                if (result == null) {
                    throw new RuntimeException("Invalid bare query");
                }
                return result;
            }
            throw new RuntimeException("Invalid atom clause: missing field or bare query");
        }

        @Override
        public QsNode visitBareQuery(SearchParser.BareQueryContext ctx) {
            // Use currentFieldName if inside a field group context (set by visitFieldGroupQuery),
            // otherwise fall back to the configured defaultField.
            String effectiveField = (currentFieldName != null && !currentFieldName.isEmpty())
                    ? currentFieldName : defaultField;
            if (effectiveField == null || effectiveField.isEmpty()) {
                throw new SearchDslSyntaxException(
                    "No field specified and no default_field configured. "
                    + "Either use field:value syntax or set default_field in options.");
            }

            fieldNames.add(effectiveField);

            // Set current field context before visiting search value
            String previousFieldName = currentFieldName;
            currentFieldName = effectiveField;

            try {
                if (ctx.searchValue() == null) {
                    throw new RuntimeException("Invalid bare query: missing search value");
                }
                QsNode result = visit(ctx.searchValue());
                if (result == null) {
                    throw new RuntimeException("Invalid search value");
                }
                return result;
            } finally {
                // Restore previous context
                currentFieldName = previousFieldName;
            }
        }

        @Override
        public QsNode visitFieldQuery(SearchParser.FieldQueryContext ctx) {
            if (ctx.fieldPath() == null) {
                throw new RuntimeException("Invalid field query: missing field path");
            }

            // Build complete field path from segments (support field.subcolumn syntax)
            StringBuilder fullPath = new StringBuilder();
            List<SearchParser.FieldSegmentContext> segments = ctx.fieldPath().fieldSegment();

            for (int i = 0; i < segments.size(); i++) {
                if (i > 0) {
                    fullPath.append('.');
                }
                String segment = segments.get(i).getText();
                // Remove quotes if present
                if (segment.startsWith("\"") && segment.endsWith("\"")) {
                    segment = segment.substring(1, segment.length() - 1);
                }
                fullPath.append(segment);
            }

            String fieldPath = fullPath.toString();
            fieldNames.add(fieldPath);

            // Set current field context before visiting search value
            String previousFieldName = currentFieldName;
            currentFieldName = fieldPath;

            try {
                if (ctx.searchValue() == null) {
                    throw new RuntimeException("Invalid field query: missing search value");
                }
                QsNode result = visit(ctx.searchValue());
                if (result == null) {
                    throw new RuntimeException("Invalid search value");
                }
                // Mark as explicit field - user wrote "field:term" syntax
                result.setExplicitField(true);
                return result;
            } finally {
                // Restore previous context
                currentFieldName = previousFieldName;
            }
        }

        @Override
        public QsNode visitFieldGroupQuery(SearchParser.FieldGroupQueryContext ctx) {
            if (ctx.fieldPath() == null) {
                throw new SearchDslSyntaxException("Invalid field group query: missing field path");
            }

            // Build complete field path from segments (support field.subcolumn syntax)
            StringBuilder fullPath = new StringBuilder();
            List<SearchParser.FieldSegmentContext> segments = ctx.fieldPath().fieldSegment();
            for (int i = 0; i < segments.size(); i++) {
                if (i > 0) {
                    fullPath.append('.');
                }
                String segment = segments.get(i).getText();
                if (segment.startsWith("\"") && segment.endsWith("\"")) {
                    segment = segment.substring(1, segment.length() - 1);
                }
                fullPath.append(segment);
            }

            String fieldPath = fullPath.toString();
            fieldNames.add(fieldPath);

            // Set field group context so bare terms inside use this field
            String previousFieldName = currentFieldName;
            currentFieldName = fieldPath;

            try {
                if (ctx.clause() == null) {
                    throw new SearchDslSyntaxException("Invalid field group query: missing inner clause");
                }
                QsNode result = visit(ctx.clause());
                if (result == null) {
                    throw new SearchDslSyntaxException("Invalid field group query: inner clause returned null");
                }
                // Mark all leaf nodes as explicitly bound to this field.
                // This prevents MultiFieldExpander from re-expanding them across other fields.
                markExplicitFieldRecursive(result, fieldPath);
                return result;
            } finally {
                currentFieldName = previousFieldName;
            }
        }

        @Override
        public QsNode visitSearchValue(SearchParser.SearchValueContext ctx) {
            String fieldName = getCurrentFieldName();

            // Handle each search value type independently for better readability
            if (ctx.TERM() != null) {
                return createTermNode(fieldName, ctx.TERM().getText());
            }

            if (ctx.PREFIX() != null) {
                return createPrefixNode(fieldName, ctx.PREFIX().getText());
            }

            if (ctx.WILDCARD() != null) {
                return createWildcardNode(fieldName, ctx.WILDCARD().getText());
            }

            if (ctx.REGEXP() != null) {
                return createRegexpNode(fieldName, ctx.REGEXP().getText());
            }

            if (ctx.QUOTED() != null) {
                return createPhraseNode(fieldName, ctx.QUOTED().getText());
            }

            if (ctx.rangeValue() != null) {
                return createRangeNode(fieldName, ctx.rangeValue());
            }

            if (ctx.listValue() != null) {
                return createListNode(fieldName, ctx.listValue());
            }

            if (ctx.anyAllValue() != null) {
                return createAnyAllNode(fieldName, ctx.anyAllValue().getText());
            }

            if (ctx.exactValue() != null) {
                return createExactNode(fieldName, ctx.exactValue().getText());
            }

            // Fallback for unknown types - should not normally reach here
            LOG.warn("Unexpected search value type encountered, falling back to TERM: field={}, text={}",
                    fieldName, ctx.getText());
            return createTermNode(fieldName, ctx.getText());
        }

        private QsNode createTermNode(String fieldName, String value) {
            return new QsNode(QsClauseType.TERM, fieldName, unescapeTermValue(value));
        }

        private QsNode createPrefixNode(String fieldName, String value) {
            // Standalone * → MATCH_ALL_DOCS (matches ES behavior: field:* becomes ExistsQuery)
            if ("*".equals(value)) {
                return new QsNode(QsClauseType.MATCH_ALL_DOCS, (List<QsNode>) null);
            }
            return new QsNode(QsClauseType.PREFIX, fieldName, unescapeTermValue(value));
        }

        private QsNode createWildcardNode(String fieldName, String value) {
            return new QsNode(QsClauseType.WILDCARD, fieldName, unescapeTermValue(value));
        }

        private QsNode createRegexpNode(String fieldName, String regexpText) {
            String regexp = regexpText;
            // Remove surrounding slashes
            if (regexp.startsWith("/") && regexp.endsWith("/")) {
                regexp = regexp.substring(1, regexp.length() - 1);
            }
            return new QsNode(QsClauseType.REGEXP, fieldName, regexp);
        }

        private QsNode createPhraseNode(String fieldName, String quotedText) {
            String quoted = quotedText;
            // Remove surrounding quotes
            if (quoted.startsWith("\"") && quoted.endsWith("\"")) {
                quoted = quoted.substring(1, quoted.length() - 1);
            }
            return new QsNode(QsClauseType.PHRASE, fieldName, quoted);
        }

        private QsNode createRangeNode(String fieldName, SearchParser.RangeValueContext ctx) {
            // Reconstruct the original range text with spaces preserved
            String rangeText;
            if (ctx.LBRACKET() != null) {
                rangeText = "[" + ctx.rangeEndpoint(0).getText() + " TO " + ctx.rangeEndpoint(1).getText() + "]";
            } else {
                rangeText = "{" + ctx.rangeEndpoint(0).getText() + " TO " + ctx.rangeEndpoint(1).getText() + "}";
            }
            return new QsNode(QsClauseType.RANGE, fieldName, rangeText);
        }

        private QsNode createListNode(String fieldName, SearchParser.ListValueContext ctx) {
            // Reconstruct the original list text with spaces preserved
            StringBuilder listText = new StringBuilder("IN(");
            for (int i = 0; i < ctx.LIST_TERM().size(); i++) {
                if (i > 0) {
                    listText.append(" ");
                }
                listText.append(ctx.LIST_TERM(i).getText());
            }
            listText.append(")");
            return new QsNode(QsClauseType.LIST, fieldName, listText.toString());
        }

        private QsNode createAnyAllNode(String fieldName, String anyAllText) {
            // Extract content between parentheses
            String innerContent = extractParenthesesContent(anyAllText);
            String sanitizedContent = stripOuterQuotes(innerContent);

            if (anyAllText.toUpperCase().startsWith("ANY(") || anyAllText.toLowerCase().startsWith("any(")) {
                return new QsNode(QsClauseType.ANY, fieldName, sanitizedContent);
            }

            if (anyAllText.toUpperCase().startsWith("ALL(") || anyAllText.toLowerCase().startsWith("all(")) {
                return new QsNode(QsClauseType.ALL, fieldName, sanitizedContent);
            }

            // Unknown ANY/ALL clause type - this should not happen with valid grammar
            throw new IllegalArgumentException(
                    "Unknown ANY/ALL clause type: '" + anyAllText + "'. "
                    + "Expected ANY(...) or ALL(...).");
        }

        private QsNode createExactNode(String fieldName, String exactText) {
            // Extract content between parentheses
            String innerContent = extractParenthesesContent(exactText);
            return new QsNode(QsClauseType.EXACT, fieldName, innerContent);
        }

        private String extractParenthesesContent(String text) {
            int openParen = text.indexOf('(');
            int closeParen = text.lastIndexOf(')');
            if (openParen >= 0 && closeParen > openParen) {
                return text.substring(openParen + 1, closeParen).trim();
            }
            return "";
        }

        private String getCurrentFieldName() {
            // Use the current field name from parsing context
            if (currentFieldName != null) {
                return currentFieldName;
            }
            // Fall back to default field if set
            if (defaultField != null && !defaultField.isEmpty()) {
                return defaultField;
            }
            // This should not happen if visitBareQuery is called correctly
            throw new SearchDslSyntaxException(
                "No field name available. This indicates a parsing error.");
        }

        private String stripOuterQuotes(String text) {
            if (text == null || text.length() < 2) {
                return text;
            }
            char first = text.charAt(0);
            char last = text.charAt(text.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return text.substring(1, text.length() - 1);
            }
            return text;
        }
    }

    /**
     * Intermediate Representation for search DSL parsing result.
     * This class is immutable after construction.
     */
    public static class QsPlan {
        @JsonProperty("root")
        private final QsNode root;

        @JsonProperty("fieldBindings")
        private final List<QsFieldBinding> fieldBindings;

        @JsonProperty("defaultOperator")
        private final String defaultOperator;

        @JsonProperty("minimumShouldMatch")
        private final Integer minimumShouldMatch;

        @JsonCreator
        public QsPlan(@JsonProperty("root") QsNode root,
                @JsonProperty("fieldBindings") List<QsFieldBinding> fieldBindings) {
            this(root, fieldBindings, null, null);
        }

        public QsPlan(QsNode root, List<QsFieldBinding> fieldBindings, String defaultOperator) {
            this(root, fieldBindings, defaultOperator, null);
        }

        public QsPlan(QsNode root, List<QsFieldBinding> fieldBindings, String defaultOperator,
                Integer minimumShouldMatch) {
            this.root = Objects.requireNonNull(root, "root cannot be null");
            this.fieldBindings = fieldBindings != null ? new ArrayList<>(fieldBindings) : new ArrayList<>();
            this.defaultOperator = defaultOperator;
            this.minimumShouldMatch = minimumShouldMatch;
        }

        public QsNode getRoot() {
            return root;
        }

        public List<QsFieldBinding> getFieldBindings() {
            return Collections.unmodifiableList(fieldBindings);
        }

        public String getDefaultOperator() {
            return defaultOperator;
        }

        public Integer getMinimumShouldMatch() {
            return minimumShouldMatch;
        }

        /**
         * Parse QsPlan from JSON string
         */
        public static QsPlan fromJson(String json) {
            try {
                return JSON_MAPPER.readValue(json, QsPlan.class);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(
                        "Failed to parse search plan from JSON: " + e.getMessage(), e);
            }
        }

        /**
         * Serialize QsPlan to JSON string
         */
        public String toJson() {
            try {
                return JSON_MAPPER.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize QsPlan to JSON", e);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(root, fieldBindings, defaultOperator, minimumShouldMatch);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QsPlan qsPlan = (QsPlan) o;
            return Objects.equals(root, qsPlan.getRoot())
                    && Objects.equals(fieldBindings, qsPlan.getFieldBindings())
                    && Objects.equals(defaultOperator, qsPlan.getDefaultOperator())
                    && Objects.equals(minimumShouldMatch, qsPlan.getMinimumShouldMatch());
        }
    }

    /**
     * Search AST node representing a clause in the DSL.
     *
     * <p><b>Warning:</b> This class is mutable. The {@code occur}, {@code children},
     * and other fields can be modified after construction. Although this class implements
     * {@code equals()} and {@code hashCode()}, it should NOT be used as a key in
     * {@code HashMap} or element in {@code HashSet} if any field may be modified after
     * insertion, as this will break the hash-based collection contract.
     */
    public static class QsNode {
        @JsonProperty("type")
        private final QsClauseType type;

        @JsonProperty("field")
        private String field;

        @JsonProperty("value")
        private final String value;

        @JsonProperty("children")
        private final List<QsNode> children;

        @JsonProperty("occur")
        private QsOccur occur;

        @JsonProperty("minimumShouldMatch")
        private final Integer minimumShouldMatch;

        /**
         * Whether the field was explicitly specified in the DSL syntax (e.g., title:music)
         * vs assigned from default field for bare queries (e.g., music).
         * Used internally by MultiFieldExpander to avoid expanding explicit field prefixes.
         * Not serialized to JSON since it's only needed during FE-side AST expansion.
         */
        @JsonIgnore
        private boolean explicitField;

        /**
         * Constructor for JSON deserialization
         *
         * @param type the clause type
         * @param field the field name
         * @param value the field value
         * @param children the child nodes
         * @param occur the occurrence type
         * @param minimumShouldMatch the minimum should match value
         */
        @JsonCreator
        public QsNode(@JsonProperty("type") QsClauseType type,
                @JsonProperty("field") String field,
                @JsonProperty("value") String value,
                @JsonProperty("children") List<QsNode> children,
                @JsonProperty("occur") QsOccur occur,
                @JsonProperty("minimumShouldMatch") Integer minimumShouldMatch) {
            this.type = type;
            this.field = field;
            this.value = value;
            this.children = children != null ? new ArrayList<>(children) : new ArrayList<>();
            this.occur = occur;
            this.minimumShouldMatch = minimumShouldMatch;
        }

        /**
         * Constructor for leaf nodes (TERM, PHRASE, PREFIX, etc.)
         *
         * @param type the clause type
         * @param field the field name
         * @param value the field value
         */
        public QsNode(QsClauseType type, String field, String value) {
            this.type = type;
            this.field = field;
            this.value = value;
            this.children = new ArrayList<>();
            this.occur = null;
            this.minimumShouldMatch = null;
        }

        /**
         * Constructor for compound nodes (AND, OR, NOT)
         *
         * @param type the clause type
         * @param children the child nodes
         */
        public QsNode(QsClauseType type, List<QsNode> children) {
            this.type = type;
            this.field = null;
            this.value = null;
            this.children = children != null ? new ArrayList<>(children) : new ArrayList<>();
            this.occur = null;
            this.minimumShouldMatch = null;
        }

        /**
         * Constructor for OCCUR_BOOLEAN nodes with minimum_should_match
         *
         * @param type the clause type
         * @param children the child nodes
         * @param minimumShouldMatch the minimum number of SHOULD clauses that must match
         */
        public QsNode(QsClauseType type, List<QsNode> children, Integer minimumShouldMatch) {
            this.type = type;
            this.field = null;
            this.value = null;
            this.children = children != null ? new ArrayList<>(children) : new ArrayList<>();
            this.occur = null;
            this.minimumShouldMatch = minimumShouldMatch;
        }

        public QsClauseType getType() {
            return type;
        }

        public String getField() {
            return field;
        }

        /**
         * Sets the field name for this node (used for field name normalization).
         * @param field the normalized field name
         */
        public void setField(String field) {
            this.field = field;
        }

        public String getValue() {
            return value;
        }

        public List<QsNode> getChildren() {
            return Collections.unmodifiableList(children);
        }

        public QsOccur getOccur() {
            return occur;
        }

        public Integer getMinimumShouldMatch() {
            return minimumShouldMatch;
        }

        /**
         * Returns whether the field was explicitly specified in the DSL syntax.
         */
        public boolean isExplicitField() {
            return explicitField;
        }

        /**
         * Sets whether the field was explicitly specified in the DSL syntax.
         * @param explicitField true if field was explicitly specified (e.g., title:music)
         * @return this node for method chaining
         */
        public QsNode setExplicitField(boolean explicitField) {
            this.explicitField = explicitField;
            return this;
        }

        /**
         * Sets the occur type for this node.
         * @param occur the occur type (MUST, SHOULD, MUST_NOT)
         * @return this node for method chaining
         */
        public QsNode setOccur(QsOccur occur) {
            this.occur = occur;
            return this;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, field, value, children, occur, minimumShouldMatch);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QsNode qsNode = (QsNode) o;
            return type == qsNode.getType()
                    && Objects.equals(field, qsNode.getField())
                    && Objects.equals(value, qsNode.getValue())
                    && Objects.equals(children, qsNode.getChildren())
                    && occur == qsNode.getOccur()
                    && Objects.equals(minimumShouldMatch, qsNode.getMinimumShouldMatch());
        }
    }

    /**
     * Field binding information extracted from DSL.
     * The fieldName may be modified for normalization purposes.
     */
    public static class QsFieldBinding {
        @JsonProperty("fieldName")
        private String fieldName;

        @JsonProperty("slotIndex")
        private final int slotIndex;

        @JsonCreator
        public QsFieldBinding(@JsonProperty("fieldName") String fieldName,
                @JsonProperty("slotIndex") int slotIndex) {
            this.fieldName = fieldName;
            this.slotIndex = slotIndex;
        }

        public String getFieldName() {
            return fieldName;
        }

        /**
         * Sets the field name (used for field name normalization).
         * @param fieldName the normalized field name
         */
        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public int getSlotIndex() {
            return slotIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, slotIndex);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QsFieldBinding that = (QsFieldBinding) o;
            return slotIndex == that.slotIndex
                    && Objects.equals(fieldName, that.fieldName);
        }
    }

    /**
     * Multi-field AST expander.
     * Transforms AST nodes with bareQuery (using placeholder field) into proper multi-field queries.
     * Supports two strategies:
     * - cross_fields: each term expands to (field1:term OR field2:term)
     * - best_fields: entire query copied per field, joined with OR
     */
    private static class MultiFieldExpander {

        /**
         * Expand AST using cross_fields strategy.
         * Each leaf node becomes OR of that node across all fields.
         * Example: "hello AND world" with fields=[title,content] becomes
         *   (title:hello OR content:hello) AND (title:world OR content:world)
         *
         * @param root The AST root node
         * @param fields List of fields to expand across
         * @return Expanded AST
         */
        public static QsNode expandCrossFields(QsNode root, List<String> fields) {
            return expandCrossFields(root, fields, false);
        }

        /**
         * Expand AST using cross_fields strategy with optional Lucene mode.
         */
        public static QsNode expandCrossFields(QsNode root, List<String> fields, boolean luceneMode) {
            if (fields == null || fields.isEmpty()) {
                return root;
            }
            if (fields.size() == 1) {
                // Single field - just set the field on all leaf nodes
                return setFieldOnLeaves(root, fields.get(0), fields);
            }
            return expandNodeCrossFields(root, fields, luceneMode);
        }

        /**
         * Expand AST using best_fields strategy.
         * Entire query is copied for each field, joined with OR.
         * Example: "hello AND world" with fields=[title,content] becomes
         *   (title:hello AND title:world) OR (content:hello AND content:world)
         *
         * @param root The AST root node
         * @param fields List of fields to expand across
         * @return Expanded AST
         */
        public static QsNode expandBestFields(QsNode root, List<String> fields) {
            if (fields == null || fields.isEmpty()) {
                return root;
            }
            if (fields.size() == 1) {
                return setFieldOnLeaves(root, fields.get(0), fields);
            }

            // Non-lucene mode (used by parseDslMultiFieldMode for multi_match semantics):
            // Copy entire AST per field, join with OR.
            // Example: "hello AND world" with fields=[title,content] becomes
            //   (title:hello AND title:world) OR (content:hello AND content:world)
            List<QsNode> fieldTrees = new ArrayList<>();
            for (String field : fields) {
                QsNode copy = deepCopyWithField(root, field, fields);
                fieldTrees.add(copy);
            }
            return new QsNode(QsClauseType.OR, fieldTrees);
        }

        /**
         * Recursively expand a node using cross_fields strategy.
         * Always returns a new copy or new node structure, never the original node.
         */
        private static QsNode expandNodeCrossFields(QsNode node, List<String> fields, boolean luceneMode) {
            // MATCH_ALL_DOCS matches all documents regardless of field - don't expand
            if (node.getType() == QsClauseType.MATCH_ALL_DOCS) {
                return new QsNode(QsClauseType.MATCH_ALL_DOCS, (List<QsNode>) null);
            }

            // Check if this is a leaf node (no children)
            if (isLeafNode(node)) {
                // If the user explicitly wrote "field:term" syntax, respect it - don't expand
                if (node.isExplicitField()) {
                    return new QsNode(
                            node.getType(),
                            node.getField(),
                            node.getValue(),
                            null,
                            node.getOccur(),
                            node.getMinimumShouldMatch()
                    );
                }

                // Expand leaf node across all fields
                List<QsNode> expandedNodes = new ArrayList<>();
                for (String field : fields) {
                    // Create complete copy with new field
                    QsNode copy = new QsNode(
                            node.getType(),
                            field,
                            node.getValue(),
                            null,
                            luceneMode ? QsOccur.SHOULD : null,  // In Lucene mode, set SHOULD
                            node.getMinimumShouldMatch()
                    );
                    expandedNodes.add(copy);
                }

                // In Lucene mode, create OCCUR_BOOLEAN with parent occur
                // Otherwise create OR node
                if (luceneMode) {
                    QsNode result = new QsNode(QsClauseType.OCCUR_BOOLEAN, expandedNodes, null);
                    if (node.getOccur() != null) {
                        result.setOccur(node.getOccur());
                    }
                    return result;
                } else {
                    return new QsNode(QsClauseType.OR, expandedNodes);
                }
            }

            // Compound node - recursively expand children
            List<QsNode> expandedChildren = new ArrayList<>();
            if (node.getChildren() != null) {
                for (QsNode child : node.getChildren()) {
                    expandedChildren.add(expandNodeCrossFields(child, fields, luceneMode));
                }
            }

            // Create new compound node with expanded children (always a copy)
            QsNode result = new QsNode(
                    node.getType(),
                    node.getField(),
                    node.getValue(),
                    expandedChildren,
                    node.getOccur(),
                    node.getMinimumShouldMatch()
            );
            return result;
        }

        /**
         * Check if a node is a leaf node (no children, representing a term/phrase/etc.)
         * A leaf node has no children or empty children list, regardless of whether it has a value.
         */
        private static boolean isLeafNode(QsNode node) {
            return node.getChildren() == null || node.getChildren().isEmpty();
        }

        /**
         * Deep copy an AST node and set the field on leaf nodes.
         * Preserves explicit fields that are not in the fields list.
         * Always returns a new copy, never the original node.
         */
        private static QsNode deepCopyWithField(QsNode node, String field, List<String> fields) {
            // MATCH_ALL_DOCS matches all documents regardless of field - don't set field
            if (node.getType() == QsClauseType.MATCH_ALL_DOCS) {
                return new QsNode(QsClauseType.MATCH_ALL_DOCS, (List<QsNode>) null);
            }
            if (isLeafNode(node)) {
                // If the user explicitly wrote "field:term" syntax, preserve original field
                String targetField = node.isExplicitField() ? node.getField() : field;

                // Create a complete copy of the leaf node
                QsNode copy = new QsNode(
                        node.getType(),
                        targetField,
                        node.getValue(),
                        null,  // children
                        node.getOccur(),
                        node.getMinimumShouldMatch()
                );
                copy.setExplicitField(node.isExplicitField());
                return copy;
            }

            // Compound node - recursively copy children
            List<QsNode> copiedChildren = new ArrayList<>();
            if (node.getChildren() != null) {
                for (QsNode child : node.getChildren()) {
                    copiedChildren.add(deepCopyWithField(child, field, fields));
                }
            }

            // Create a complete copy of the compound node
            QsNode result = new QsNode(
                    node.getType(),
                    node.getField(),
                    node.getValue(),
                    copiedChildren,
                    node.getOccur(),
                    node.getMinimumShouldMatch()
            );
            return result;
        }

        /**
         * Set field on leaf nodes (for single-field case).
         * Preserves explicit fields that are different from the target field.
         * Always returns a new copy, never the original node.
         */
        private static QsNode setFieldOnLeaves(QsNode node, String field, List<String> fields) {
            // MATCH_ALL_DOCS matches all documents regardless of field - don't set field
            if (node.getType() == QsClauseType.MATCH_ALL_DOCS) {
                return new QsNode(QsClauseType.MATCH_ALL_DOCS, (List<QsNode>) null);
            }
            if (isLeafNode(node)) {
                // If the user explicitly wrote "field:term" syntax, preserve original field
                String targetField = node.isExplicitField() ? node.getField() : field;

                // Create complete copy
                return new QsNode(
                        node.getType(),
                        targetField,
                        node.getValue(),
                        null,
                        node.getOccur(),
                        node.getMinimumShouldMatch()
                );
            }

            // Compound node - recursively process children
            List<QsNode> updatedChildren = new ArrayList<>();
            if (node.getChildren() != null) {
                for (QsNode child : node.getChildren()) {
                    updatedChildren.add(setFieldOnLeaves(child, field, fields));
                }
            }

            // Create complete copy
            return new QsNode(
                    node.getType(),
                    node.getField(),
                    node.getValue(),
                    updatedChildren,
                    node.getOccur(),
                    node.getMinimumShouldMatch()
            );
        }
    }

    /**
     * Search options parsed from JSON.
     * Supports all configuration in a single JSON object:
     * - default_field: default field name when DSL doesn't specify field
     * - default_operator: "and" or "or" for multi-term queries (default: "or")
     * - mode: "standard" (default) or "lucene" (ES/Lucene-style boolean parsing)
     * - minimum_should_match: integer for Lucene mode (default: 0 for filter context)
     * - fields: array of field names for multi-field search (mutually exclusive with default_field)
     */
    public static class SearchOptions {
        @JsonProperty("default_field")
        private String defaultField = null;

        @JsonProperty("default_operator")
        private String defaultOperator = null;

        @JsonProperty("mode")
        private String mode = "standard";

        @JsonProperty("minimum_should_match")
        private Integer minimumShouldMatch = null;

        private List<String> fields = null;

        @JsonProperty("type")
        private String type = "best_fields";  // "best_fields" (default) or "cross_fields"

        public String getDefaultField() {
            return defaultField;
        }

        public void setDefaultField(String defaultField) {
            this.defaultField = defaultField;
        }

        public String getDefaultOperator() {
            return defaultOperator;
        }

        public void setDefaultOperator(String defaultOperator) {
            this.defaultOperator = defaultOperator;
        }

        public boolean isLuceneMode() {
            return "lucene".equalsIgnoreCase(mode);
        }

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public Integer getMinimumShouldMatch() {
            return minimumShouldMatch;
        }

        public void setMinimumShouldMatch(Integer minimumShouldMatch) {
            this.minimumShouldMatch = minimumShouldMatch;
        }

        public List<String> getFields() {
            return fields == null ? null : Collections.unmodifiableList(fields);
        }

        /**
         * Set fields with empty element filtering.
         * Empty or whitespace-only strings are filtered out.
         */
        @JsonSetter("fields")
        public void setFields(List<String> fields) {
            if (fields == null) {
                this.fields = null;
                return;
            }
            // Filter out empty or whitespace-only elements
            List<String> filtered = fields.stream()
                    .filter(f -> f != null && !f.trim().isEmpty())
                    .map(String::trim)
                    .collect(Collectors.toList());
            this.fields = filtered.isEmpty() ? null : new ArrayList<>(filtered);
        }

        /**
         * Check if multi-field mode is enabled.
         * Multi-field mode is active when fields array is non-null and non-empty.
         */
        public boolean isMultiFieldMode() {
            return fields != null && !fields.isEmpty();
        }

        /**
         * Get the multi-field search type ("best_fields" or "cross_fields").
         */
        public String getType() {
            return type;
        }

        /**
         * Set the multi-field search type.
         * @param type Either "best_fields" or "cross_fields" (case-insensitive)
         * @throws IllegalArgumentException if type is invalid
         */
        public void setType(String type) {
            if (type == null) {
                this.type = "best_fields";
                return;
            }
            String normalized = type.trim().toLowerCase();
            if (!"cross_fields".equals(normalized) && !"best_fields".equals(normalized)) {
                throw new IllegalArgumentException(
                        "'type' must be 'cross_fields' or 'best_fields', got: " + type);
            }
            this.type = normalized;
        }

        /**
         * Check if best_fields mode is enabled (default).
         * In best_fields mode, all terms must match within the same field.
         */
        public boolean isBestFieldsMode() {
            return "best_fields".equals(type);
        }

        /**
         * Check if cross_fields mode is enabled.
         * In cross_fields mode, terms can match across different fields.
         */
        public boolean isCrossFieldsMode() {
            return "cross_fields".equals(type);
        }

        /**
         * Create a copy of this SearchOptions with a different minimum_should_match value.
         * Used for ES compatibility in multi-field mode where msm is ignored.
         */
        public SearchOptions withMinimumShouldMatch(Integer newMsm) {
            SearchOptions copy = new SearchOptions();
            copy.defaultField = this.defaultField;
            copy.defaultOperator = this.defaultOperator;
            copy.mode = this.mode;
            copy.minimumShouldMatch = newMsm;
            copy.fields = this.fields != null ? new ArrayList<>(this.fields) : null;
            copy.type = this.type;
            return copy;
        }

        /**
         * Validate the options after deserialization.
         * Checks for:
         * - mode is "standard" or "lucene"
         * - Mutual exclusion between fields and default_field
         * - minimum_should_match is non-negative if specified
         *
         * @throws IllegalArgumentException if validation fails
         */
        public void validate() {
            // Validation: mode must be "standard" or "lucene"
            if (mode != null) {
                String normalizedMode = mode.trim().toLowerCase();
                if (!"standard".equals(normalizedMode) && !"lucene".equals(normalizedMode)) {
                    throw new IllegalArgumentException(
                            "'mode' must be 'standard' or 'lucene', got: " + mode);
                }
                this.mode = normalizedMode;
            }
            // Validation: fields and default_field are mutually exclusive
            if (fields != null && !fields.isEmpty()
                    && defaultField != null && !defaultField.isEmpty()) {
                throw new IllegalArgumentException(
                        "'fields' and 'default_field' are mutually exclusive. Use only one.");
            }
            // Validation: minimum_should_match should be non-negative
            if (minimumShouldMatch != null && minimumShouldMatch < 0) {
                throw new IllegalArgumentException(
                        "'minimum_should_match' must be non-negative, got: " + minimumShouldMatch);
            }
        }
    }

    /**
     * Parse options JSON string using Jackson databind.
     * The SearchOptions class uses @JsonProperty annotations for field mapping
     * and @JsonSetter for custom deserialization logic (e.g., filtering empty fields).
     *
     * @param optionsJson JSON string containing search options
     * @return Parsed and validated SearchOptions
     * @throws IllegalArgumentException if JSON is invalid or validation fails
     */
    private static SearchOptions parseOptions(String optionsJson) {
        if (optionsJson == null || optionsJson.trim().isEmpty()) {
            return new SearchOptions();
        }

        try {
            // Use Jackson to deserialize directly into SearchOptions
            // @JsonProperty annotations handle field mapping
            // @JsonSetter on setFields() handles empty element filtering
            SearchOptions options = JSON_MAPPER.readValue(optionsJson, SearchOptions.class);
            // Run validation checks (mutual exclusion, range checks, etc.)
            options.validate();
            return options;
        } catch (IllegalArgumentException e) {
            // Re-throw validation errors as-is
            throw e;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Invalid search options JSON: '" + optionsJson + "'. Error: " + e.getMessage(), e);
        }
    }

    /**
     * Lucene mode parsing - implements ES/Lucene-style boolean query semantics.
     * <p>
     * Key differences from standard mode:
     * - Operators are processed left-to-right as modifiers (not traditional boolean algebra)
     * - AND marks preceding and current terms as MUST (+)
     * - OR marks preceding and current terms as SHOULD
     * - NOT marks current term as MUST_NOT (-)
     * - When minimum_should_match=0 and there are MUST clauses, SHOULD clauses are ignored
     * <p>
     * Examples:
     * - "a AND b OR c" → +a (with minimum_should_match=0, SHOULD terms discarded)
     * - "a AND b OR NOT c AND d" → +a -c +d
     * - "a OR b OR c" → a b c (at least one must match)
     */
    private static QsPlan parseDslLuceneMode(String dsl, String defaultField, String defaultOperator,
            SearchOptions options) {
        if (dsl == null || dsl.trim().isEmpty()) {
            return new QsPlan(new QsNode(QsClauseType.TERM, "error", "empty_dsl"), new ArrayList<>());
        }

        // Parse original DSL directly - no preprocessing needed
        // The ANTLR grammar now supports bareQuery (without field prefix)
        // and QsLuceneModeAstBuilder will use defaultField from options for bare queries
        String trimmedDsl = dsl.trim();

        try {
            // Create ANTLR lexer and parser
            SearchLexer lexer = new SearchLexer(CharStreams.fromString(trimmedDsl));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SearchParser parser = new SearchParser(tokens);

            // Add error listener
            parser.removeErrorListeners();
            parser.addErrorListener(new org.antlr.v4.runtime.BaseErrorListener() {
                @Override
                public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                        Object offendingSymbol,
                        int line, int charPositionInLine,
                        String msg, org.antlr.v4.runtime.RecognitionException e) {
                    throw new SearchDslSyntaxException("Syntax error at line " + line
                            + ":" + charPositionInLine + " " + msg);
                }
            });

            // Parse using standard parser first
            ParseTree tree = parser.search();
            if (tree == null) {
                throw new SearchDslSyntaxException("Invalid search DSL syntax: parsing returned null");
            }

            // Build AST using Lucene-mode visitor
            QsLuceneModeAstBuilder visitor = new QsLuceneModeAstBuilder(options);
            QsNode root = visitor.visit(tree);

            // Extract field bindings
            Set<String> fieldNames = visitor.getFieldNames();
            List<QsFieldBinding> bindings = new ArrayList<>();
            int slotIndex = 0;
            for (String fieldName : fieldNames) {
                bindings.add(new QsFieldBinding(fieldName, slotIndex++));
            }

            // Include default_operator and minimum_should_match for BE
            return new QsPlan(root, bindings,
                    normalizeDefaultOperator(defaultOperator),
                    options.getMinimumShouldMatch());

        } catch (SearchDslSyntaxException e) {
            // Syntax error in DSL - user input issue
            LOG.error("Failed to parse search DSL in Lucene mode: '{}'", dsl, e);
            throw new SearchDslSyntaxException("Invalid search DSL: " + dsl + ". " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            // Invalid argument - user input issue
            LOG.error("Invalid argument in search DSL (Lucene mode): '{}'", dsl, e);
            throw new IllegalArgumentException("Invalid search DSL argument: " + dsl + ". " + e.getMessage(), e);
        } catch (NullPointerException e) {
            // Internal error - programming bug
            LOG.error("Internal error (NPE) while parsing search DSL in Lucene mode: '{}'", dsl, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + dsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (IndexOutOfBoundsException e) {
            // Internal error - programming bug
            LOG.error("Internal error (IOOB) while parsing search DSL in Lucene mode: '{}'", dsl, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + dsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            // Other runtime errors
            LOG.error("Unexpected error while parsing search DSL in Lucene mode: '{}'", dsl, e);
            throw new RuntimeException("Unexpected error parsing search DSL: " + dsl + ". " + e.getMessage(), e);
        }
    }

    /**
     * ANTLR visitor for Lucene-mode AST building.
     * Transforms standard boolean expressions into Lucene-style OCCUR_BOOLEAN queries.
     */
    private static class QsLuceneModeAstBuilder extends SearchParserBaseVisitor<QsNode>
            implements FieldTrackingVisitor {
        private final Set<String> fieldNames = new LinkedHashSet<>();
        private final SearchOptions options;
        private String currentFieldName = null;
        // Override for default field - used in multi-field mode to avoid mutating options
        private final String overrideDefaultField;
        private int nestingLevel = 0;

        public QsLuceneModeAstBuilder(SearchOptions options) {
            this.options = options;
            this.overrideDefaultField = null;
        }

        /**
         * Constructor with override default field for multi-field mode.
         * This avoids mutating the shared SearchOptions object.
         * @param options Search options
         * @param overrideDefaultField Field to use as default instead of options.getDefaultField()
         */
        public QsLuceneModeAstBuilder(SearchOptions options, String overrideDefaultField) {
            this.options = options;
            this.overrideDefaultField = overrideDefaultField;
        }

        /**
         * Get the effective default field, considering override.
         */
        private String getEffectiveDefaultField() {
            if (overrideDefaultField != null && !overrideDefaultField.isEmpty()) {
                return overrideDefaultField;
            }
            return options != null ? options.getDefaultField() : null;
        }

        public Set<String> getFieldNames() {
            return Collections.unmodifiableSet(fieldNames);
        }

        @Override
        public QsNode visitSearch(SearchParser.SearchContext ctx) {
            QsNode result = visit(ctx.clause());
            if (result == null) {
                throw new RuntimeException("Invalid search clause");
            }
            return result;
        }

        @Override
        public QsNode visitOrClause(SearchParser.OrClauseContext ctx) {
            // In Lucene mode, we need to process the entire OR chain together
            // to correctly assign MUST/SHOULD/MUST_NOT based on operator sequence
            return processLuceneBooleanChain(ctx);
        }

        /**
         * Process the entire boolean expression chain in Lucene mode.
         * This is the core of Lucene-style boolean parsing.
         */
        private QsNode processLuceneBooleanChain(SearchParser.OrClauseContext ctx) {
            // Collect all terms and operators from the expression tree
            List<TermWithOccur> terms = new ArrayList<>();
            collectTermsWithOperators(ctx, terms, QsOccur.MUST); // default_operator = AND means MUST

            if (terms.isEmpty()) {
                throw new RuntimeException("No terms found in boolean expression");
            }

            if (terms.size() == 1) {
                TermWithOccur singleTerm = terms.get(0);
                if (singleTerm.isNegated) {
                    // Single negated term - rewrite to: SHOULD(MATCH_ALL_DOCS) + MUST_NOT(term)
                    // This ensures proper Lucene semantics: match all docs EXCEPT those matching the term
                    singleTerm.node.setOccur(QsOccur.MUST_NOT);

                    QsNode matchAllNode = new QsNode(QsClauseType.MATCH_ALL_DOCS, (List<QsNode>) null);
                    matchAllNode.setOccur(QsOccur.SHOULD);

                    List<QsNode> children = new ArrayList<>();
                    children.add(matchAllNode);
                    children.add(singleTerm.node);
                    return new QsNode(QsClauseType.OCCUR_BOOLEAN, children, 1);
                }
                // Single non-negated term - return directly without wrapper
                return singleTerm.node;
            }

            // Apply Lucene boolean logic
            applyLuceneBooleanLogic(terms);

            // Determine minimum_should_match
            // Only use explicit option at top level; nested clauses use default logic
            Integer minShouldMatch = (nestingLevel == 0) ? options.getMinimumShouldMatch() : null;
            if (minShouldMatch == null) {
                // Default: 0 if there are MUST clauses, 1 if only SHOULD
                // This matches Lucene BooleanQuery default behavior
                boolean hasMust = terms.stream().anyMatch(t -> t.occur == QsOccur.MUST);
                boolean hasMustNot = terms.stream().anyMatch(t -> t.occur == QsOccur.MUST_NOT);
                minShouldMatch = (hasMust || hasMustNot) ? 0 : 1;
            }

            final int finalMinShouldMatch = minShouldMatch;

            if (terms.size() == 1) {
                TermWithOccur remainingTerm = terms.get(0);
                if (remainingTerm.occur == QsOccur.MUST_NOT) {
                    // Single MUST_NOT term - rewrite to: SHOULD(MATCH_ALL_DOCS) + MUST_NOT(term)
                    // This ensures proper Lucene semantics: match all docs EXCEPT those matching the term
                    remainingTerm.node.setOccur(QsOccur.MUST_NOT);

                    QsNode matchAllNode = new QsNode(QsClauseType.MATCH_ALL_DOCS, (List<QsNode>) null);
                    matchAllNode.setOccur(QsOccur.SHOULD);

                    List<QsNode> children = new ArrayList<>();
                    children.add(matchAllNode);
                    children.add(remainingTerm.node);
                    return new QsNode(QsClauseType.OCCUR_BOOLEAN, children, 1);
                }
                return remainingTerm.node;
            }

            // Build OCCUR_BOOLEAN node
            List<QsNode> children = new ArrayList<>();
            for (TermWithOccur term : terms) {
                term.node.setOccur(term.occur);
                children.add(term.node);
            }

            return new QsNode(QsClauseType.OCCUR_BOOLEAN, children, finalMinShouldMatch);
        }

        /**
         * Collect terms from the parse tree with their positions
         */
        private void collectTermsWithOperators(ParseTree ctx, List<TermWithOccur> terms, QsOccur defaultOccur) {
            if (ctx instanceof SearchParser.OrClauseContext) {
                SearchParser.OrClauseContext orCtx = (SearchParser.OrClauseContext) ctx;
                List<SearchParser.AndClauseContext> andClauses = orCtx.andClause();

                for (int i = 0; i < andClauses.size(); i++) {
                    // Mark that this term is introduced by OR if not the first
                    boolean introducedByOr = (i > 0);
                    collectTermsFromAndClause(andClauses.get(i), terms, defaultOccur, introducedByOr);
                }
            }
        }

        private void collectTermsFromAndClause(SearchParser.AndClauseContext ctx, List<TermWithOccur> terms,
                QsOccur defaultOccur, boolean introducedByOr) {
            List<SearchParser.NotClauseContext> notClauses = ctx.notClause();

            for (int i = 0; i < notClauses.size(); i++) {
                boolean introducedByAnd;
                if (i > 0) {
                    // Check if there's an explicit AND token before this notClause.
                    // Implicit conjunction (no AND token) returns false - only explicit AND
                    // should trigger the "introduced by AND" logic that modifies preceding terms.
                    introducedByAnd = hasExplicitAndBefore(ctx, notClauses.get(i));
                } else {
                    introducedByAnd = false;
                }

                collectTermsFromNotClause(notClauses.get(i), terms, defaultOccur, introducedByOr, introducedByAnd);
                // After first term, all subsequent in same AND chain are introducedByOr=false
                introducedByOr = false;
            }
        }

        /**
         * Check if there's an explicit AND token before the target notClause.
         * Walks ctx.children to find the position of target and checks the preceding token.
         *
         * IMPORTANT: Returns false for implicit conjunction (no explicit AND token).
         * In Lucene's QueryParserBase.addClause(), only explicit CONJ_AND modifies the
         * preceding term. CONJ_NONE (implicit conjunction) only affects the current term's
         * occur via the default_operator, without modifying the preceding term.
         *
         * @param ctx The AndClauseContext containing the children
         * @param target The target NotClauseContext to check
         * @return true only if there's an explicit AND token before target
         */
        private boolean hasExplicitAndBefore(SearchParser.AndClauseContext ctx,
                SearchParser.NotClauseContext target) {
            for (int j = 0; j < ctx.getChildCount(); j++) {
                if (ctx.getChild(j) == target) {
                    // Found the target - check if the preceding sibling is an AND token
                    if (j > 0 && ctx.getChild(j - 1) instanceof org.antlr.v4.runtime.tree.TerminalNode) {
                        org.antlr.v4.runtime.tree.TerminalNode terminal =
                                (org.antlr.v4.runtime.tree.TerminalNode) ctx.getChild(j - 1);
                        return terminal.getSymbol().getType() == SearchParser.AND;
                    }
                    // No explicit AND before this term
                    return false;
                }
            }
            // Target not found (should not happen)
            return false;
        }

        private void collectTermsFromNotClause(SearchParser.NotClauseContext ctx, List<TermWithOccur> terms,
                QsOccur defaultOccur, boolean introducedByOr, boolean introducedByAnd) {
            boolean isNegated = ctx.NOT() != null;
            SearchParser.AtomClauseContext atomCtx = ctx.atomClause();

            QsNode node;
            if (atomCtx.clause() != null) {
                // Parenthesized clause - visit recursively with incremented nesting level
                // This ensures nested clauses don't use top-level minimum_should_match
                nestingLevel++;
                try {
                    node = visit(atomCtx.clause());
                } finally {
                    nestingLevel--;
                }
            } else if (atomCtx.fieldGroupQuery() != null) {
                // Field group query (e.g., title:(rock OR jazz))
                node = visit(atomCtx.fieldGroupQuery());
            } else if (atomCtx.fieldQuery() != null) {
                // Field query with explicit field prefix
                node = visit(atomCtx.fieldQuery());
            } else if (atomCtx.bareQuery() != null) {
                // Bare query - uses default field
                node = visit(atomCtx.bareQuery());
            } else {
                throw new RuntimeException("Invalid atom clause: missing field or bare query");
            }

            TermWithOccur term = new TermWithOccur(node, defaultOccur);
            term.introducedByOr = introducedByOr;
            term.introducedByAnd = introducedByAnd;
            term.isNegated = isNegated;
            terms.add(term);
        }

        /**
         * Apply Lucene boolean logic to determine final MUST/SHOULD/MUST_NOT for each term.
         * <p>
         * Faithfully replicates Lucene QueryParserBase.addClause() semantics:
         * - Processes terms left-to-right with NO operator precedence (AND/OR are equal)
         * - Each conjunction affects at most the immediately preceding term
         * <p>
         * With OR_OPERATOR (default_operator=OR):
         *   - First term / no conjunction: SHOULD
         *   - AND: preceding becomes MUST, current MUST
         *   - OR: current SHOULD (preceding unchanged)
         * <p>
         * With AND_OPERATOR (default_operator=AND):
         *   - First term / no conjunction: MUST
         *   - AND: preceding becomes MUST, current MUST
         *   - OR: preceding becomes SHOULD, current SHOULD
         */
        private void applyLuceneBooleanLogic(List<TermWithOccur> terms) {
            boolean useAnd = "AND".equalsIgnoreCase(options.getDefaultOperator());

            for (int i = 0; i < terms.size(); i++) {
                TermWithOccur current = terms.get(i);

                if (current.isNegated) {
                    // NOT modifier - mark as MUST_NOT
                    current.occur = QsOccur.MUST_NOT;

                    if (current.introducedByAnd && i > 0) {
                        // AND + NOT: AND still makes preceding MUST
                        TermWithOccur prev = terms.get(i - 1);
                        if (prev.occur != QsOccur.MUST_NOT) {
                            prev.occur = QsOccur.MUST;
                        }
                    } else if (current.introducedByOr && i > 0 && useAnd) {
                        // OR + NOT with AND_OPERATOR: preceding becomes SHOULD
                        TermWithOccur prev = terms.get(i - 1);
                        if (prev.occur != QsOccur.MUST_NOT) {
                            prev.occur = QsOccur.SHOULD;
                        }
                    }
                    // OR + NOT with OR_OPERATOR: no change to preceding
                } else if (current.introducedByAnd) {
                    // AND: preceding becomes MUST, current MUST
                    current.occur = QsOccur.MUST;
                    if (i > 0) {
                        TermWithOccur prev = terms.get(i - 1);
                        if (prev.occur != QsOccur.MUST_NOT) {
                            prev.occur = QsOccur.MUST;
                        }
                    }
                } else if (current.introducedByOr) {
                    // OR: current is SHOULD
                    current.occur = QsOccur.SHOULD;
                    // Only change preceding to SHOULD if default_operator=AND
                    // (Lucene: OR_OPERATOR + CONJ_OR does NOT modify preceding)
                    if (useAnd && i > 0) {
                        TermWithOccur prev = terms.get(i - 1);
                        if (prev.occur != QsOccur.MUST_NOT) {
                            prev.occur = QsOccur.SHOULD;
                        }
                    }
                } else {
                    // First term or implicit conjunction (no explicit AND/OR)
                    // Lucene: SHOULD for OR_OPERATOR, MUST for AND_OPERATOR
                    current.occur = useAnd ? QsOccur.MUST : QsOccur.SHOULD;
                }
            }
        }

        @Override
        public QsNode visitAndClause(SearchParser.AndClauseContext ctx) {
            // This is called for simple cases, delegate to parent's logic
            if (ctx.notClause().size() == 1) {
                return visit(ctx.notClause(0));
            }

            // Multiple AND terms - use processLuceneBooleanChain via parent
            List<QsNode> children = new ArrayList<>();
            for (SearchParser.NotClauseContext notCtx : ctx.notClause()) {
                QsNode child = visit(notCtx);
                if (child != null) {
                    children.add(child);
                }
            }

            if (children.size() == 1) {
                return children.get(0);
            }

            // Check if there are explicit AND tokens
            // If no explicit AND tokens, use the default operator for implicit conjunction
            List<org.antlr.v4.runtime.tree.TerminalNode> andTokens = ctx.AND();
            boolean hasExplicitAnd = andTokens != null && !andTokens.isEmpty();

            QsClauseType clauseType;
            if (hasExplicitAnd) {
                // Explicit AND - always use AND
                clauseType = QsClauseType.AND;
            } else {
                // Implicit conjunction - use default operator from options
                String defaultOperator = options.getDefaultOperator();
                clauseType = "AND".equalsIgnoreCase(defaultOperator) ? QsClauseType.AND : QsClauseType.OR;
            }

            return new QsNode(clauseType, children);
        }

        @Override
        public QsNode visitNotClause(SearchParser.NotClauseContext ctx) {
            if (ctx.NOT() != null) {
                QsNode child = visit(ctx.atomClause());
                if (child == null) {
                    throw new RuntimeException("Invalid NOT clause: missing operand");
                }
                List<QsNode> children = new ArrayList<>();
                children.add(child);
                return new QsNode(QsClauseType.NOT, children);
            }
            return visit(ctx.atomClause());
        }

        @Override
        public QsNode visitAtomClause(SearchParser.AtomClauseContext ctx) {
            if (ctx.clause() != null) {
                return visit(ctx.clause());
            }
            if (ctx.fieldGroupQuery() != null) {
                return visit(ctx.fieldGroupQuery());
            }
            if (ctx.fieldQuery() != null) {
                return visit(ctx.fieldQuery());
            }
            if (ctx.bareQuery() != null) {
                return visit(ctx.bareQuery());
            }
            throw new RuntimeException("Invalid atom clause: missing field or bare query");
        }

        @Override
        public QsNode visitBareQuery(SearchParser.BareQueryContext ctx) {
            // Use currentFieldName if inside a field group context (set by visitFieldGroupQuery),
            // otherwise fall back to the effective default field.
            String defaultField = getEffectiveDefaultField();
            String effectiveField = (currentFieldName != null && !currentFieldName.isEmpty())
                    ? currentFieldName : defaultField;
            if (effectiveField == null || effectiveField.isEmpty()) {
                throw new SearchDslSyntaxException(
                    "No field specified and no default_field configured. "
                    + "Either use field:value syntax or set default_field in options.");
            }

            fieldNames.add(effectiveField);

            // Set current field context before visiting search value
            String previousFieldName = currentFieldName;
            currentFieldName = effectiveField;

            try {
                if (ctx.searchValue() == null) {
                    throw new RuntimeException("Invalid bare query: missing search value");
                }
                QsNode result = visit(ctx.searchValue());
                if (result == null) {
                    throw new RuntimeException("Invalid search value");
                }
                return result;
            } finally {
                // Restore previous context
                currentFieldName = previousFieldName;
            }
        }

        @Override
        public QsNode visitFieldQuery(SearchParser.FieldQueryContext ctx) {
            // Build complete field path
            StringBuilder fullPath = new StringBuilder();
            List<SearchParser.FieldSegmentContext> segments = ctx.fieldPath().fieldSegment();

            for (int i = 0; i < segments.size(); i++) {
                if (i > 0) {
                    fullPath.append('.');
                }
                String segment = segments.get(i).getText();
                if (segment.startsWith("\"") && segment.endsWith("\"")) {
                    segment = segment.substring(1, segment.length() - 1);
                }
                fullPath.append(segment);
            }

            String fieldPath = fullPath.toString();
            fieldNames.add(fieldPath);

            String previousFieldName = currentFieldName;
            currentFieldName = fieldPath;

            try {
                QsNode result = visit(ctx.searchValue());
                // Mark as explicit field - user wrote "field:term" syntax
                result.setExplicitField(true);
                return result;
            } finally {
                currentFieldName = previousFieldName;
            }
        }

        @Override
        public QsNode visitFieldGroupQuery(SearchParser.FieldGroupQueryContext ctx) {
            if (ctx.fieldPath() == null) {
                throw new SearchDslSyntaxException("Invalid field group query: missing field path");
            }

            // Build complete field path from segments (support field.subcolumn syntax)
            StringBuilder fullPath = new StringBuilder();
            List<SearchParser.FieldSegmentContext> segments = ctx.fieldPath().fieldSegment();
            for (int i = 0; i < segments.size(); i++) {
                if (i > 0) {
                    fullPath.append('.');
                }
                String segment = segments.get(i).getText();
                if (segment.startsWith("\"") && segment.endsWith("\"")) {
                    segment = segment.substring(1, segment.length() - 1);
                }
                fullPath.append(segment);
            }

            String fieldPath = fullPath.toString();
            fieldNames.add(fieldPath);

            // Set field group context so bare terms inside use this field
            String previousFieldName = currentFieldName;
            currentFieldName = fieldPath;
            nestingLevel++;

            try {
                if (ctx.clause() == null) {
                    throw new SearchDslSyntaxException("Invalid field group query: missing inner clause");
                }
                QsNode result = visit(ctx.clause());
                if (result == null) {
                    throw new SearchDslSyntaxException("Invalid field group query: inner clause returned null");
                }
                // Mark all leaf nodes as explicitly bound to this field.
                // This prevents MultiFieldExpander from re-expanding them across other fields.
                markExplicitFieldRecursive(result, fieldPath);
                return result;
            } finally {
                nestingLevel--;
                currentFieldName = previousFieldName;
            }
        }

        @Override
        public QsNode visitSearchValue(SearchParser.SearchValueContext ctx) {
            String fieldName = currentFieldName;
            if (fieldName == null) {
                // Fall back to effective default field (considering override)
                String defaultField = getEffectiveDefaultField();
                if (defaultField != null && !defaultField.isEmpty()) {
                    fieldName = defaultField;
                } else {
                    throw new SearchDslSyntaxException(
                        "No field name available. This indicates a parsing error.");
                }
            }

            if (ctx.TERM() != null) {
                return new QsNode(QsClauseType.TERM, fieldName, unescapeTermValue(ctx.TERM().getText()));
            }
            if (ctx.PREFIX() != null) {
                String prefixText = ctx.PREFIX().getText();
                // Standalone * → MATCH_ALL_DOCS (matches ES behavior: field:* becomes ExistsQuery)
                if ("*".equals(prefixText)) {
                    return new QsNode(QsClauseType.MATCH_ALL_DOCS, (List<QsNode>) null);
                }
                return new QsNode(QsClauseType.PREFIX, fieldName, unescapeTermValue(prefixText));
            }
            if (ctx.WILDCARD() != null) {
                return new QsNode(QsClauseType.WILDCARD, fieldName, unescapeTermValue(ctx.WILDCARD().getText()));
            }
            if (ctx.REGEXP() != null) {
                String regexp = ctx.REGEXP().getText();
                if (regexp.startsWith("/") && regexp.endsWith("/")) {
                    regexp = regexp.substring(1, regexp.length() - 1);
                }
                return new QsNode(QsClauseType.REGEXP, fieldName, regexp);
            }
            if (ctx.QUOTED() != null) {
                String quoted = ctx.QUOTED().getText();
                if (quoted.startsWith("\"") && quoted.endsWith("\"")) {
                    quoted = quoted.substring(1, quoted.length() - 1);
                }
                return new QsNode(QsClauseType.PHRASE, fieldName, quoted);
            }
            if (ctx.rangeValue() != null) {
                SearchParser.RangeValueContext rangeCtx = ctx.rangeValue();
                String rangeText;
                if (rangeCtx.LBRACKET() != null) {
                    rangeText = "[" + rangeCtx.rangeEndpoint(0).getText() + " TO "
                            + rangeCtx.rangeEndpoint(1).getText() + "]";
                } else {
                    rangeText = "{" + rangeCtx.rangeEndpoint(0).getText() + " TO "
                            + rangeCtx.rangeEndpoint(1).getText() + "}";
                }
                return new QsNode(QsClauseType.RANGE, fieldName, rangeText);
            }
            if (ctx.listValue() != null) {
                StringBuilder listText = new StringBuilder("IN(");
                for (int i = 0; i < ctx.listValue().LIST_TERM().size(); i++) {
                    if (i > 0) {
                        listText.append(" ");
                    }
                    listText.append(ctx.listValue().LIST_TERM(i).getText());
                }
                listText.append(")");
                return new QsNode(QsClauseType.LIST, fieldName, listText.toString());
            }
            if (ctx.anyAllValue() != null) {
                String text = ctx.anyAllValue().getText();
                String innerContent = extractParenthesesContent(text);
                String sanitizedContent = stripOuterQuotes(innerContent);
                if (text.toUpperCase().startsWith("ANY(")) {
                    return new QsNode(QsClauseType.ANY, fieldName, sanitizedContent);
                }
                return new QsNode(QsClauseType.ALL, fieldName, sanitizedContent);
            }
            if (ctx.exactValue() != null) {
                String innerContent = extractParenthesesContent(ctx.exactValue().getText());
                return new QsNode(QsClauseType.EXACT, fieldName, innerContent);
            }

            return new QsNode(QsClauseType.TERM, fieldName, unescapeTermValue(ctx.getText()));
        }

        private String extractParenthesesContent(String text) {
            int openParen = text.indexOf('(');
            int closeParen = text.lastIndexOf(')');
            if (openParen >= 0 && closeParen > openParen) {
                return text.substring(openParen + 1, closeParen).trim();
            }
            return "";
        }

        private String stripOuterQuotes(String text) {
            if (text == null || text.length() < 2) {
                return text;
            }
            char first = text.charAt(0);
            char last = text.charAt(text.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return text.substring(1, text.length() - 1);
            }
            return text;
        }
    }

    /**
     * Helper class to track term with its occur status during parsing
     */
    private static class TermWithOccur {
        QsNode node;
        QsOccur occur;
        boolean introducedByOr = false;
        boolean introducedByAnd = false;
        boolean isNegated = false;

        TermWithOccur(QsNode node, QsOccur occur) {
            this.node = node;
            this.occur = occur;
        }
    }

    /**
     * Process escape sequences in a term value.
     * Converts escape sequences to their literal characters:
     * - \  (backslash space) -> space
     * - \( -> (
     * - \) -> )
     * - \: -> :
     * - \\ -> \
     * - \* -> *
     * - \? -> ?
     * - etc.
     *
     * @param value The raw term value with escape sequences
     * @return The unescaped value
     */
    private static String unescapeTermValue(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        // Quick check: if no backslash, return as-is
        if (value.indexOf('\\') < 0) {
            return value;
        }

        StringBuilder result = new StringBuilder(value.length());
        int i = 0;
        while (i < value.length()) {
            char c = value.charAt(i);
            if (c == '\\' && i + 1 < value.length()) {
                // Escape sequence - take the next character literally
                result.append(value.charAt(i + 1));
                i += 2;
            } else {
                result.append(c);
                i++;
            }
        }
        return result.toString();
    }
}
