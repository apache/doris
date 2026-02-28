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

        // Use Lucene mode parser if specified
        if (searchOptions.isLuceneMode()) {
            // Multi-field + Lucene mode: first expand DSL, then parse with Lucene semantics
            if (searchOptions.isMultiFieldMode()) {
                return parseDslMultiFieldLuceneMode(dsl, searchOptions.getFields(),
                        defaultOperator, searchOptions);
            }
            return parseDslLuceneMode(dsl, defaultField, defaultOperator, searchOptions);
        }

        // Multi-field mode parsing (standard mode)
        if (searchOptions.isMultiFieldMode()) {
            return parseDslMultiFieldMode(dsl, searchOptions.getFields(), defaultOperator, searchOptions);
        }

        // Standard mode parsing
        return parseDslStandardMode(dsl, defaultField, defaultOperator);
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
     */
    private static QsPlan parseDslStandardMode(String dsl, String defaultField, String defaultOperator) {
        if (dsl == null || dsl.trim().isEmpty()) {
            return new QsPlan(new QsNode(QsClauseType.TERM, "error", "empty_dsl"), new ArrayList<>());
        }

        // Expand simplified DSL if default field is provided
        String expandedDsl = dsl;
        if (defaultField != null && !defaultField.trim().isEmpty()) {
            expandedDsl = expandSimplifiedDsl(dsl.trim(), defaultField.trim(),
                    normalizeDefaultOperator(defaultOperator));
        }

        try {
            // Create ANTLR lexer and parser
            SearchLexer lexer = new SearchLexer(CharStreams.fromString(expandedDsl));
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

            // Build AST using visitor pattern
            QsAstBuilder visitor = new QsAstBuilder();
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
            LOG.error("Failed to parse search DSL: '{}' (expanded: '{}')", dsl, expandedDsl, e);
            throw new SearchDslSyntaxException("Invalid search DSL: " + dsl + ". " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            // Invalid argument - user input issue
            LOG.error("Invalid argument in search DSL: '{}' (expanded: '{}')", dsl, expandedDsl, e);
            throw new IllegalArgumentException("Invalid search DSL argument: " + dsl + ". " + e.getMessage(), e);
        } catch (NullPointerException e) {
            // Internal error - programming bug
            LOG.error("Internal error (NPE) while parsing search DSL: '{}' (expanded: '{}')", dsl, expandedDsl, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + dsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (IndexOutOfBoundsException e) {
            // Internal error - programming bug
            LOG.error("Internal error (IOOB) while parsing search DSL: '{}' (expanded: '{}')", dsl, expandedDsl, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + dsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            // Other runtime errors
            LOG.error("Unexpected error while parsing search DSL: '{}' (expanded: '{}')", dsl, expandedDsl, e);
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

    /**
     * Expand simplified DSL to full DSL format
     * <p>
     * Examples:
     * - "foo bar" + field="tags" + operator="and" → "tags:ALL(foo bar)"
     * - "foo* bar*" + field="tags" + operator="and" → "tags:foo* AND tags:bar*"
     * - "foo OR bar" + field="tags" → "tags:foo OR tags:bar"
     * - "EXACT(foo bar)" + field="tags" → "tags:EXACT(foo bar)"
     *
     * @param dsl Simple DSL string
     * @param defaultField Default field name
     * @param defaultOperator "and" or "or"
     * @return Expanded full DSL
     */
    private static String expandSimplifiedDsl(String dsl, String defaultField, String defaultOperator) {
        // 1. If DSL already contains field names (colon), return as-is
        if (containsFieldReference(dsl)) {
            return dsl;
        }

        // 2. Check if DSL starts with a function keyword (EXACT, ANY, ALL, IN)
        if (startsWithFunction(dsl)) {
            return defaultField + ":" + dsl;
        }

        // 3. Check for explicit boolean operators in DSL
        if (containsExplicitOperators(dsl)) {
            return addFieldPrefixToOperatorExpression(dsl, defaultField);
        }

        // 4. Tokenize and analyze terms
        List<String> terms = tokenizeDsl(dsl);
        if (terms.isEmpty()) {
            return defaultField + ":" + dsl;
        }

        // 5. Single term - simple case
        if (terms.size() == 1) {
            return defaultField + ":" + terms.get(0);
        }

        // 6. Multiple terms - check for wildcards
        boolean hasWildcard = terms.stream().anyMatch(SearchDslParser::containsWildcard);

        if (hasWildcard) {
            // Wildcards cannot be tokenized - must create separate field queries
            String operator = "and".equals(defaultOperator) ? " AND " : " OR ";
            return terms.stream()
                    .map(term -> defaultField + ":" + term)
                    .collect(java.util.stream.Collectors.joining(operator));
        } else {
            // Regular multi-term query - use ANY/ALL
            String clauseType = "and".equals(defaultOperator) ? "ALL" : "ANY";
            return defaultField + ":" + clauseType + "(" + dsl + ")";
        }
    }

    /**
     * Check if DSL contains field references (has colon not in quoted strings or escaped)
     */
    private static boolean containsFieldReference(String dsl) {
        boolean inQuotes = false;
        boolean inRegex = false;
        for (int i = 0; i < dsl.length(); i++) {
            char c = dsl.charAt(i);
            // Handle escape sequences - skip the escaped character
            if (c == '\\' && i + 1 < dsl.length()) {
                i++; // Skip next character (it's escaped)
                continue;
            }
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == '/' && !inQuotes) {
                inRegex = !inRegex;
            } else if (c == ':' && !inQuotes && !inRegex) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if DSL starts with function keywords
     */
    private static boolean startsWithFunction(String dsl) {
        String upper = dsl.toUpperCase();
        return upper.startsWith("EXACT(")
                || upper.startsWith("ANY(")
                || upper.startsWith("ALL(")
                || upper.startsWith("IN(");
    }

    /**
     * Check if DSL contains explicit boolean operators (AND/OR/NOT)
     */
    private static boolean containsExplicitOperators(String dsl) {
        // Look for standalone AND/OR/NOT keywords (not part of field names)
        String upper = dsl.toUpperCase();
        return upper.matches(".*\\s+(AND|OR)\\s+.*")
                || upper.matches("^NOT\\s+.*")
                || upper.matches(".*\\s+NOT\\s+.*");
    }

    /**
     * Add field prefix to expressions with explicit operators
     * Example: "foo AND bar" → "field:foo AND field:bar"
     * Handles escape sequences properly (e.g., "First\ Value" stays as single term)
     */
    private static String addFieldPrefixToOperatorExpression(String dsl, String defaultField) {
        StringBuilder result = new StringBuilder();
        StringBuilder currentTerm = new StringBuilder();
        int i = 0;

        while (i < dsl.length()) {
            // Skip whitespace (but not escaped whitespace)
            while (i < dsl.length() && Character.isWhitespace(dsl.charAt(i))) {
                i++;
            }
            if (i >= dsl.length()) {
                break;
            }

            // Handle escape sequences - include both backslash and next char
            if (dsl.charAt(i) == '\\' && i + 1 < dsl.length()) {
                currentTerm.append(dsl.charAt(i));
                currentTerm.append(dsl.charAt(i + 1));
                i += 2;
                continue;
            }

            // Try to match operators
            String remaining = dsl.substring(i);
            String upperRemaining = remaining.toUpperCase();

            if (upperRemaining.startsWith("AND ") || upperRemaining.startsWith("AND\t")
                    || (upperRemaining.equals("AND") && i + 3 >= dsl.length())) {
                // Found AND operator
                if (currentTerm.length() > 0) {
                    if (result.length() > 0) {
                        result.append(" ");
                    }
                    result.append(defaultField).append(":").append(currentTerm.toString().trim());
                    currentTerm.setLength(0);
                }
                if (result.length() > 0) {
                    result.append(" ");
                }
                result.append(dsl.substring(i, i + 3)); // Preserve original case
                i += 3;
                continue;
            } else if (upperRemaining.startsWith("OR ") || upperRemaining.startsWith("OR\t")
                    || (upperRemaining.equals("OR") && i + 2 >= dsl.length())) {
                // Found OR operator
                if (currentTerm.length() > 0) {
                    if (result.length() > 0) {
                        result.append(" ");
                    }
                    result.append(defaultField).append(":").append(currentTerm.toString().trim());
                    currentTerm.setLength(0);
                }
                if (result.length() > 0) {
                    result.append(" ");
                }
                result.append(dsl.substring(i, i + 2)); // Preserve original case
                i += 2;
                continue;
            } else if (upperRemaining.startsWith("NOT ") || upperRemaining.startsWith("NOT\t")
                    || (upperRemaining.equals("NOT") && i + 3 >= dsl.length())) {
                // Found NOT operator
                if (currentTerm.length() > 0) {
                    if (result.length() > 0) {
                        result.append(" ");
                    }
                    result.append(defaultField).append(":").append(currentTerm.toString().trim());
                    currentTerm.setLength(0);
                }
                if (result.length() > 0) {
                    result.append(" ");
                }
                result.append(dsl.substring(i, i + 3)); // Preserve original case
                i += 3;
                continue;
            }

            // Not an operator, accumulate term
            currentTerm.append(dsl.charAt(i));
            i++;
        }

        // Add last term
        if (currentTerm.length() > 0) {
            if (result.length() > 0) {
                result.append(" ");
            }
            result.append(defaultField).append(":").append(currentTerm.toString().trim());
        }

        return result.toString().trim();
    }

    /**
     * Tokenize DSL into terms (split by whitespace, respecting quotes, escapes, and functions)
     */
    private static List<String> tokenizeDsl(String dsl) {
        List<String> terms = new ArrayList<>();
        StringBuilder currentTerm = new StringBuilder();
        boolean inQuotes = false;
        boolean inParens = false;
        int parenDepth = 0;

        for (int i = 0; i < dsl.length(); i++) {
            char c = dsl.charAt(i);

            if (c == '"' && (i == 0 || dsl.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
                currentTerm.append(c);
            } else if (c == '(' && !inQuotes) {
                parenDepth++;
                inParens = true;
                currentTerm.append(c);
            } else if (c == ')' && !inQuotes) {
                parenDepth--;
                if (parenDepth == 0) {
                    inParens = false;
                }
                currentTerm.append(c);
            } else if (c == '\\' && i + 1 < dsl.length()) {
                // Escape sequence - include both backslash and next char in term
                currentTerm.append(c);
                currentTerm.append(dsl.charAt(i + 1));
                i++; // Skip next character
            } else if (Character.isWhitespace(c) && !inQuotes && !inParens) {
                // End of term (only if not escaped - handled above)
                if (currentTerm.length() > 0) {
                    terms.add(currentTerm.toString());
                    currentTerm.setLength(0);  // Reuse StringBuilder instead of creating new one
                }
            } else {
                currentTerm.append(c);
            }
        }

        // Add last term
        if (currentTerm.length() > 0) {
            terms.add(currentTerm.toString());
        }

        return terms;
    }

    /**
     * Check if a term contains wildcard characters (* or ?)
     * Escaped wildcards (\* or \?) are not counted.
     */
    private static boolean containsWildcard(String term) {
        // Ignore wildcards in quoted strings or regex
        if (term.startsWith("\"") && term.endsWith("\"")) {
            return false;
        }
        if (term.startsWith("/") && term.endsWith("/")) {
            return false;
        }
        // Check for unescaped wildcards
        for (int i = 0; i < term.length(); i++) {
            char c = term.charAt(i);
            if (c == '\\' && i + 1 < term.length()) {
                // Skip escaped character
                i++;
                continue;
            }
            if (c == '*' || c == '?') {
                return true;
            }
        }
        return false;
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
     * Expand a single item (term or function) across multiple fields with OR.
     * Example: "hello" + ["title", "content"] -> "(title:hello OR content:hello)"
     * Example: "EXACT(foo)" + ["title", "content"] -> "(title:EXACT(foo) OR content:EXACT(foo))"
     *
     * @param item The term or function to expand
     * @param fields List of field names
     * @return Expanded DSL string
     */
    private static String expandItemAcrossFields(String item, List<String> fields) {
        if (fields.size() == 1) {
            return fields.get(0) + ":" + item;
        }
        return fields.stream()
                .map(field -> field + ":" + item)
                .collect(Collectors.joining(" OR ", "(", ")"));
    }

    // ============ Multi-Field Expansion Methods ============

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

        String normalizedOperator = normalizeDefaultOperator(defaultOperator);
        String expandedDsl;
        if (options.isCrossFieldsMode()) {
            // cross_fields: terms can be across different fields
            expandedDsl = expandMultiFieldDsl(dsl.trim(), fields, normalizedOperator);
        } else if (options.isBestFieldsMode()) {
            // best_fields: all terms must be in the same field
            expandedDsl = expandMultiFieldDslBestFields(dsl.trim(), fields, normalizedOperator);
        } else {
            // Should never happen due to setType() validation, but provide fallback
            throw new IllegalStateException(
                    "Invalid type value: '" + options.getType() + "'. Expected 'best_fields' or 'cross_fields'");
        }
        return parseWithVisitor(expandedDsl, parser -> new QsAstBuilder(), dsl, "multi-field mode");
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

        String normalizedOperator = normalizeDefaultOperator(defaultOperator);
        String expandedDsl;
        if (options.isCrossFieldsMode()) {
            // cross_fields: terms can be across different fields
            expandedDsl = expandMultiFieldDsl(dsl.trim(), fields, normalizedOperator);
        } else if (options.isBestFieldsMode()) {
            // best_fields: all terms must be in the same field
            expandedDsl = expandMultiFieldDslBestFields(dsl.trim(), fields, normalizedOperator);
        } else {
            // Should never happen due to setType() validation, but provide fallback
            throw new IllegalStateException(
                    "Invalid type value: '" + options.getType() + "'. Expected 'best_fields' or 'cross_fields'");
        }
        return parseWithVisitor(expandedDsl, parser -> new QsLuceneModeAstBuilder(options),
                dsl, "multi-field Lucene mode");
    }

    /**
     * Expand simplified DSL to multi-field format.
     * Each term without field prefix is expanded to OR across all fields.
     *
     * @param dsl Simple DSL string
     * @param fields List of field names to search
     * @param defaultOperator "and" or "or" for joining term groups
     * @return Expanded full DSL
     */
    private static String expandMultiFieldDsl(String dsl, List<String> fields, String defaultOperator) {
        // Note: fields validation is done by validateFieldsList() before calling this method
        if (fields.size() == 1) {
            // Single field - delegate to existing method
            return expandSimplifiedDsl(dsl, fields.get(0), defaultOperator);
        }

        // 1. If DSL already contains field names, handle mixed case
        if (containsFieldReference(dsl)) {
            return expandOperatorExpressionAcrossFields(dsl, fields);
        }

        // 2. Check if DSL starts with a function keyword (EXACT, ANY, ALL, IN)
        if (startsWithFunction(dsl)) {
            // Expand function across fields: EXACT(foo) -> (f1:EXACT(foo) OR f2:EXACT(foo))
            return expandFunctionAcrossFields(dsl, fields);
        }

        // 3. Check for explicit boolean operators in DSL
        if (containsExplicitOperators(dsl)) {
            return expandOperatorExpressionAcrossFields(dsl, fields);
        }

        // 4. Tokenize and analyze terms
        List<String> terms = tokenizeDsl(dsl);
        if (terms.isEmpty()) {
            return expandTermAcrossFields(dsl, fields);
        }

        // 5. Single term - expand across fields
        if (terms.size() == 1) {
            return expandTermAcrossFields(terms.get(0), fields);
        }

        // 6. Multiple terms - expand each across fields, join with operator
        String joinOperator = "and".equals(defaultOperator) ? " AND " : " OR ";
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < terms.size(); i++) {
            if (i > 0) {
                result.append(joinOperator);
            }
            result.append(expandTermAcrossFields(terms.get(i), fields));
        }
        return result.toString();
    }

    /**
     * Expand multi-field DSL using best_fields semantics.
     * Each field is wrapped with all terms joined by the default operator, then fields are ORed.
     *
     * Example: "machine learning" with fields ["title", "content"] and default_operator "and"
     * Result: (title:machine AND title:learning) OR (content:machine AND content:learning)
     *
     * @param dsl Simple DSL string
     * @param fields List of field names to search
     * @param defaultOperator "and" or "or" for joining terms within each field
     * @return Expanded full DSL with best_fields semantics
     */
    private static String expandMultiFieldDslBestFields(String dsl, List<String> fields,
            String defaultOperator) {
        // Note: fields validation is done by validateFieldsList() before calling this method
        if (fields.size() == 1) {
            // Single field - delegate to existing method
            return expandSimplifiedDsl(dsl, fields.get(0), defaultOperator);
        }

        // 1. Check for leading NOT - must use cross_fields semantics for correct negation
        // "NOT hello" should expand to "NOT (title:hello OR content:hello)"
        // rather than "(NOT title:hello) OR (NOT content:hello)" which has wrong semantics
        String trimmedDsl = dsl.trim();
        if (trimmedDsl.toUpperCase().startsWith("NOT ")
                || trimmedDsl.toUpperCase().startsWith("NOT\t")) {
            // Use cross_fields expansion for leading NOT
            return expandOperatorExpressionAcrossFields(dsl, fields);
        }

        // 2. If DSL contains field references or explicit operators, apply best_fields
        // by expanding the entire expression per field and ORing the results
        if (containsFieldReference(dsl) || containsExplicitOperators(dsl)) {
            return expandOperatorExpressionAcrossFieldsBestFields(dsl, fields, defaultOperator);
        }

        // 3. Check if DSL starts with a function keyword (EXACT, ANY, ALL, IN)
        if (startsWithFunction(dsl)) {
            // For functions, use cross_fields approach (function applied to each field)
            return expandFunctionAcrossFields(dsl, fields);
        }

        // 4. Tokenize and analyze terms
        List<String> terms = tokenizeDsl(dsl);
        if (terms.isEmpty()) {
            // Single term case - expand across fields with OR
            return expandTermAcrossFields(dsl, fields);
        }

        // 5. Single term - expand across fields with OR
        if (terms.size() == 1) {
            return expandTermAcrossFields(terms.get(0), fields);
        }

        // 6. Multiple terms - best_fields: each field with all terms, then OR across fields
        String termOperator = "and".equals(defaultOperator) ? " AND " : " OR ";

        StringBuilder result = new StringBuilder();
        for (int fieldIdx = 0; fieldIdx < fields.size(); fieldIdx++) {
            if (fieldIdx > 0) {
                result.append(" OR ");
            }

            String field = fields.get(fieldIdx);
            // Build: (field:term1 AND field:term2 AND ...)
            result.append("(");
            for (int termIdx = 0; termIdx < terms.size(); termIdx++) {
                if (termIdx > 0) {
                    result.append(termOperator);
                }
                result.append(field).append(":").append(terms.get(termIdx));
            }
            result.append(")");
        }
        return result.toString();
    }

    /**
     * Handle DSL with explicit operators using best_fields semantics.
     * For complex expressions, we group by field and OR across fields.
     */
    private static String expandOperatorExpressionAcrossFieldsBestFields(String dsl,
            List<String> fields, String defaultOperator) {
        // For expressions with explicit operators, we apply the entire expression to each field
        // and OR the results: (title:expr) OR (content:expr)
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                result.append(" OR ");
            }
            String field = fields.get(i);
            // Expand the DSL for this single field
            String fieldDsl = expandSimplifiedDsl(dsl, field, defaultOperator);
            result.append("(").append(fieldDsl).append(")");
        }
        return result.toString();
    }

    /**
     * Expand a single term across multiple fields with OR.
     * Example: "hello" + ["title", "content"] -> "(title:hello OR content:hello)"
     * Delegates to expandItemAcrossFields for DRY compliance.
     */
    private static String expandTermAcrossFields(String term, List<String> fields) {
        return expandItemAcrossFields(term, fields);
    }

    /**
     * Expand a function call across multiple fields.
     * Example: "EXACT(foo bar)" + ["title", "content"] -> "(title:EXACT(foo bar) OR content:EXACT(foo bar))"
     * Delegates to expandItemAcrossFields for DRY compliance.
     */
    private static String expandFunctionAcrossFields(String dsl, List<String> fields) {
        return expandItemAcrossFields(dsl, fields);
    }

    /**
     * Handle DSL with explicit operators (AND/OR/NOT).
     * Each operand without field prefix is expanded across fields.
     * Example: "hello AND world" + ["title", "content"] ->
     *          "(title:hello OR content:hello) AND (title:world OR content:world)"
     */
    private static String expandOperatorExpressionAcrossFields(String dsl, List<String> fields) {
        StringBuilder result = new StringBuilder();
        StringBuilder currentTerm = new StringBuilder();
        int i = 0;

        while (i < dsl.length()) {
            // Skip whitespace
            while (i < dsl.length() && Character.isWhitespace(dsl.charAt(i))) {
                i++;
            }
            if (i >= dsl.length()) {
                break;
            }

            // Handle escape sequences
            if (dsl.charAt(i) == '\\' && i + 1 < dsl.length()) {
                currentTerm.append(dsl.charAt(i));
                currentTerm.append(dsl.charAt(i + 1));
                i += 2;
                continue;
            }

            // Handle parentheses - include entire group as a term
            if (dsl.charAt(i) == '(') {
                int depth = 1;
                currentTerm.append('(');
                i++;
                while (i < dsl.length() && depth > 0) {
                    char c = dsl.charAt(i);
                    if (c == '(') {
                        depth++;
                    } else if (c == ')') {
                        depth--;
                    }
                    currentTerm.append(c);
                    i++;
                }
                continue;
            }

            // Try to match operators
            String remaining = dsl.substring(i);
            String upperRemaining = remaining.toUpperCase();

            // Check for AND operator
            if (matchesOperatorWord(upperRemaining, "AND")) {
                flushTermAcrossFields(result, currentTerm, fields);
                appendWithSpace(result, "AND");
                i += 3;
                continue;
            }

            // Check for OR operator
            if (matchesOperatorWord(upperRemaining, "OR")) {
                flushTermAcrossFields(result, currentTerm, fields);
                appendWithSpace(result, "OR");
                i += 2;
                continue;
            }

            // Check for NOT operator
            if (matchesOperatorWord(upperRemaining, "NOT")) {
                flushTermAcrossFields(result, currentTerm, fields);
                appendWithSpace(result, "NOT");
                i += 3;
                continue;
            }

            // Accumulate term character
            currentTerm.append(dsl.charAt(i));
            i++;
        }

        // Flush final term
        flushTermAcrossFields(result, currentTerm, fields);

        return result.toString().trim();
    }

    /**
     * Check if the string starts with an operator word followed by whitespace or end of string.
     */
    private static boolean matchesOperatorWord(String upper, String op) {
        if (!upper.startsWith(op)) {
            return false;
        }
        int opLen = op.length();
        // Must be followed by whitespace or end of string
        return upper.length() == opLen || Character.isWhitespace(upper.charAt(opLen));
    }

    /**
     * Flush accumulated term, expanding across fields if needed.
     */
    private static void flushTermAcrossFields(StringBuilder result, StringBuilder term, List<String> fields) {
        String trimmed = term.toString().trim();
        if (!trimmed.isEmpty()) {
            // Check if term already has a field reference
            if (containsFieldReference(trimmed)) {
                appendWithSpace(result, trimmed);
            } else if (trimmed.startsWith("(") && trimmed.endsWith(")")) {
                // Parenthesized expression - recursively expand
                String inner = trimmed.substring(1, trimmed.length() - 1).trim();
                String expanded = expandOperatorExpressionAcrossFields(inner, fields);
                appendWithSpace(result, "(" + expanded + ")");
            } else if (startsWithFunction(trimmed)) {
                // Function - expand across fields
                appendWithSpace(result, expandFunctionAcrossFields(trimmed, fields));
            } else {
                // Regular term - expand across fields
                appendWithSpace(result, expandTermAcrossFields(trimmed, fields));
            }
            term.setLength(0);
        }
    }

    /**
     * Append text to StringBuilder with a leading space if not empty.
     */
    private static void appendWithSpace(StringBuilder sb, String text) {
        if (sb.length() > 0) {
            sb.append(" ");
        }
        sb.append(text);
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
        OCCUR_BOOLEAN   // Lucene-style boolean query with MUST/SHOULD/MUST_NOT
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
            return new QsNode(QsClauseType.AND, children);
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
            if (ctx.fieldQuery() == null) {
                throw new RuntimeException("Invalid atom clause: missing field query");
            }
            QsNode result = visit(ctx.fieldQuery());
            if (result == null) {
                throw new RuntimeException("Invalid field query");
            }
            return result;
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
                return result;
            } finally {
                // Restore previous context
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
            return currentFieldName != null ? currentFieldName : "_all";
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

        @JsonCreator
        public QsPlan(@JsonProperty("root") QsNode root,
                @JsonProperty("fieldBindings") List<QsFieldBinding> fieldBindings) {
            this.root = Objects.requireNonNull(root, "root cannot be null");
            this.fieldBindings = fieldBindings != null ? new ArrayList<>(fieldBindings) : new ArrayList<>();
        }

        public QsNode getRoot() {
            return root;
        }

        public List<QsFieldBinding> getFieldBindings() {
            return Collections.unmodifiableList(fieldBindings);
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
            return Objects.hash(root, fieldBindings);
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
                    && Objects.equals(fieldBindings, qsPlan.getFieldBindings());
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

        // Expand simplified DSL if default field is provided
        String expandedDsl = dsl;
        if (defaultField != null && !defaultField.trim().isEmpty()) {
            expandedDsl = expandSimplifiedDsl(dsl.trim(), defaultField.trim(),
                    normalizeDefaultOperator(defaultOperator));
        }

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

            return new QsPlan(root, bindings);

        } catch (SearchDslSyntaxException e) {
            // Syntax error in DSL - user input issue
            LOG.error("Failed to parse search DSL in Lucene mode: '{}' (expanded: '{}')", dsl, expandedDsl, e);
            throw new SearchDslSyntaxException("Invalid search DSL: " + dsl + ". " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            // Invalid argument - user input issue
            LOG.error("Invalid argument in search DSL (Lucene mode): '{}' (expanded: '{}')", dsl, expandedDsl, e);
            throw new IllegalArgumentException("Invalid search DSL argument: " + dsl + ". " + e.getMessage(), e);
        } catch (NullPointerException e) {
            // Internal error - programming bug
            LOG.error("Internal error (NPE) while parsing search DSL in Lucene mode: '{}' (expanded: '{}')",
                    dsl, expandedDsl, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + dsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (IndexOutOfBoundsException e) {
            // Internal error - programming bug
            LOG.error("Internal error (IOOB) while parsing search DSL in Lucene mode: '{}' (expanded: '{}')",
                    dsl, expandedDsl, e);
            throw new RuntimeException("Internal error while parsing search DSL: " + dsl
                    + ". This may be a bug. Details: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            // Other runtime errors
            LOG.error("Unexpected error while parsing search DSL in Lucene mode: '{}' (expanded: '{}')",
                    dsl, expandedDsl, e);
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

        public QsLuceneModeAstBuilder(SearchOptions options) {
            this.options = options;
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
                    // Single negated term - must wrap in OCCUR_BOOLEAN for BE to handle MUST_NOT
                    singleTerm.node.setOccur(QsOccur.MUST_NOT);
                    List<QsNode> children = new ArrayList<>();
                    children.add(singleTerm.node);
                    return new QsNode(QsClauseType.OCCUR_BOOLEAN, children, 0);
                }
                // Single non-negated term - return directly without wrapper
                return singleTerm.node;
            }

            // Apply Lucene boolean logic
            applyLuceneBooleanLogic(terms);

            // Determine minimum_should_match
            Integer minShouldMatch = options.getMinimumShouldMatch();
            if (minShouldMatch == null) {
                // Default: 0 if there are MUST clauses, 1 if only SHOULD
                boolean hasMust = terms.stream().anyMatch(t -> t.occur == QsOccur.MUST);
                boolean hasMustNot = terms.stream().anyMatch(t -> t.occur == QsOccur.MUST_NOT);
                minShouldMatch = (hasMust || hasMustNot) ? 0 : 1;
            }

            // Filter out SHOULD clauses if minimum_should_match=0 and there are MUST clauses
            final int finalMinShouldMatch = minShouldMatch;
            if (minShouldMatch == 0) {
                boolean hasMust = terms.stream().anyMatch(t -> t.occur == QsOccur.MUST);
                if (hasMust) {
                    terms = terms.stream()
                            .filter(t -> t.occur != QsOccur.SHOULD)
                            .collect(Collectors.toList());
                }
            }

            if (terms.isEmpty()) {
                throw new RuntimeException("All terms filtered out in Lucene boolean logic");
            }

            if (terms.size() == 1) {
                TermWithOccur remainingTerm = terms.get(0);
                if (remainingTerm.occur == QsOccur.MUST_NOT) {
                    // Single MUST_NOT term - must wrap in OCCUR_BOOLEAN for BE to handle
                    remainingTerm.node.setOccur(QsOccur.MUST_NOT);
                    List<QsNode> children = new ArrayList<>();
                    children.add(remainingTerm.node);
                    return new QsNode(QsClauseType.OCCUR_BOOLEAN, children, 0);
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
                boolean introducedByAnd = (i > 0);
                collectTermsFromNotClause(notClauses.get(i), terms, defaultOccur, introducedByOr, introducedByAnd);
                // After first term, all subsequent in same AND chain are introducedByOr=false
                introducedByOr = false;
            }
        }

        private void collectTermsFromNotClause(SearchParser.NotClauseContext ctx, List<TermWithOccur> terms,
                QsOccur defaultOccur, boolean introducedByOr, boolean introducedByAnd) {
            boolean isNegated = ctx.NOT() != null;
            SearchParser.AtomClauseContext atomCtx = ctx.atomClause();

            if (atomCtx.clause() != null) {
                // Parenthesized clause - visit recursively
                QsNode subNode = visit(atomCtx.clause());
                TermWithOccur term = new TermWithOccur(subNode, defaultOccur);
                term.introducedByOr = introducedByOr;
                term.introducedByAnd = introducedByAnd;
                term.isNegated = isNegated;
                terms.add(term);
            } else {
                // Field query
                QsNode node = visit(atomCtx.fieldQuery());
                TermWithOccur term = new TermWithOccur(node, defaultOccur);
                term.introducedByOr = introducedByOr;
                term.introducedByAnd = introducedByAnd;
                term.isNegated = isNegated;
                terms.add(term);
            }
        }

        /**
         * Apply Lucene boolean logic to determine final MUST/SHOULD/MUST_NOT for each term.
         * <p>
         * Rules (processed left-to-right):
         * 1. First term: MUST (due to default_operator=AND)
         * 2. AND introduces: marks preceding and current as MUST
         * 3. OR introduces: marks preceding and current as SHOULD
         * 4. NOT modifier: marks current as MUST_NOT
         * 5. AND after MUST_NOT: the MUST_NOT term is not affected, current becomes MUST
         */
        private void applyLuceneBooleanLogic(List<TermWithOccur> terms) {
            for (int i = 0; i < terms.size(); i++) {
                TermWithOccur current = terms.get(i);

                if (current.isNegated) {
                    // NOT modifier - mark as MUST_NOT
                    current.occur = QsOccur.MUST_NOT;

                    // OR + NOT: preceding becomes SHOULD (if not already MUST_NOT)
                    if (current.introducedByOr && i > 0) {
                        TermWithOccur prev = terms.get(i - 1);
                        if (prev.occur != QsOccur.MUST_NOT) {
                            prev.occur = QsOccur.SHOULD;
                        }
                    }
                } else if (current.introducedByAnd) {
                    // AND introduces: both preceding and current are MUST
                    current.occur = QsOccur.MUST;
                    if (i > 0) {
                        TermWithOccur prev = terms.get(i - 1);
                        // Don't change MUST_NOT to MUST
                        if (prev.occur != QsOccur.MUST_NOT) {
                            prev.occur = QsOccur.MUST;
                        }
                    }
                } else if (current.introducedByOr) {
                    // OR introduces: both preceding and current are SHOULD
                    current.occur = QsOccur.SHOULD;
                    if (i > 0) {
                        TermWithOccur prev = terms.get(i - 1);
                        // Don't change MUST_NOT to SHOULD
                        if (prev.occur != QsOccur.MUST_NOT) {
                            prev.occur = QsOccur.SHOULD;
                        }
                    }
                } else {
                    // First term: MUST (default_operator=AND)
                    current.occur = QsOccur.MUST;
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

            return new QsNode(QsClauseType.AND, children);
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
            return visit(ctx.fieldQuery());
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
                return visit(ctx.searchValue());
            } finally {
                currentFieldName = previousFieldName;
            }
        }

        @Override
        public QsNode visitSearchValue(SearchParser.SearchValueContext ctx) {
            String fieldName = currentFieldName != null ? currentFieldName : "_all";

            if (ctx.TERM() != null) {
                return new QsNode(QsClauseType.TERM, fieldName, unescapeTermValue(ctx.TERM().getText()));
            }
            if (ctx.PREFIX() != null) {
                return new QsNode(QsClauseType.PREFIX, fieldName, unescapeTermValue(ctx.PREFIX().getText()));
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
