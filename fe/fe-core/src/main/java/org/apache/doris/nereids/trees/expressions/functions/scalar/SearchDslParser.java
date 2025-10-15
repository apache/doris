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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
     * Parse DSL string and return intermediate representation
     */
    public static QsPlan parseDsl(String dsl) {
        if (dsl == null || dsl.trim().isEmpty()) {
            return new QsPlan(new QsNode(QsClauseType.TERM, "error", "empty_dsl"), new ArrayList<>());
        }

        try {
            // Create ANTLR lexer and parser
            SearchLexer lexer = new SearchLexer(new ANTLRInputStream(dsl));
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
                    throw new RuntimeException("Invalid search DSL syntax at line " + line
                            + ":" + charPositionInLine + " " + msg);
                }
            });

            // Parse the search query
            ParseTree tree = parser.search();

            // Check if parsing was successful
            if (tree == null) {
                throw new RuntimeException("Invalid search DSL syntax");
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

        } catch (Exception e) {
            LOG.error("Failed to parse search DSL: '{}'", dsl, e);
            throw new RuntimeException("Invalid search DSL syntax: " + dsl + ". Error: " + e.getMessage(), e);
        }
    }

    /**
     * Clause types supported
     */
    public enum QsClauseType {
        TERM,       // field:value
        PHRASE,     // field:"phrase search"
        PREFIX,     // field:prefix*
        WILDCARD,   // field:*wild*card*
        REGEXP,     // field:/pattern/
        RANGE,      // field:[1 TO 10] or field:{1 TO 10}
        LIST,       // field:IN(value1 value2)
        ANY,        // field:ANY(value) - any match
        ALL,        // field:ALL(value) - all match
        EXACT,      // field:EXACT(value) - exact match without tokenization
        AND,        // clause1 AND clause2
        OR,         // clause1 OR clause2
        NOT         // NOT clause
    }

    /**
     * ANTLR visitor to build QsNode AST from parse tree
     */
    private static class QsAstBuilder extends SearchParserBaseVisitor<QsNode> {
        private final Set<String> fieldNames = new HashSet<>();
        // Context stack to track current field name during parsing
        private String currentFieldName = null;

        public Set<String> getFieldNames() {
            return fieldNames;
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
            if (ctx.fieldName() == null) {
                throw new RuntimeException("Invalid field query: missing field name");
            }
            String fieldName = ctx.fieldName().getText();
            if (fieldName.startsWith("\"") && fieldName.endsWith("\"")) {
                fieldName = fieldName.substring(1, fieldName.length() - 1);
            }
            fieldNames.add(fieldName);

            // Set current field context before visiting search value
            String previousFieldName = currentFieldName;
            currentFieldName = fieldName;

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

            // Fallback for unknown types
            return createTermNode(fieldName, ctx.getText());
        }

        private QsNode createTermNode(String fieldName, String value) {
            return new QsNode(QsClauseType.TERM, fieldName, value);
        }

        private QsNode createPrefixNode(String fieldName, String value) {
            return new QsNode(QsClauseType.PREFIX, fieldName, value);
        }

        private QsNode createWildcardNode(String fieldName, String value) {
            return new QsNode(QsClauseType.WILDCARD, fieldName, value);
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

            // Fallback to ANY for unknown cases
            return new QsNode(QsClauseType.ANY, fieldName, sanitizedContent);
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
     * Intermediate Representation for search DSL parsing result
     */
    public static class QsPlan {
        @JsonProperty("root")
        public QsNode root;

        @JsonProperty("fieldBindings")
        public List<QsFieldBinding> fieldBindings;

        @JsonCreator
        public QsPlan(@JsonProperty("root") QsNode root,
                @JsonProperty("fieldBindings") List<QsFieldBinding> fieldBindings) {
            this.root = root;
            this.fieldBindings = fieldBindings != null ? fieldBindings : new ArrayList<>();
        }

        /**
         * Parse QsPlan from JSON string
         */
        public static QsPlan fromJson(String json) {
            try {
                return JSON_MAPPER.readValue(json, QsPlan.class);
            } catch (JsonProcessingException e) {
                LOG.warn("Failed to parse QsPlan from JSON: {}", json, e);
                return new QsPlan(new QsNode(QsClauseType.TERM, "error", null), new ArrayList<>());
            }
        }

        /**
         * Serialize QsPlan to JSON string
         */
        public String toJson() {
            try {
                return JSON_MAPPER.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                LOG.warn("Failed to serialize QsPlan to JSON", e);
                return "{}";
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
            return Objects.equals(root, qsPlan.root)
                    && Objects.equals(fieldBindings, qsPlan.fieldBindings);
        }
    }

    /**
     * Search AST node representing a clause in the DSL
     */
    public static class QsNode {
        @JsonProperty("type")
        public QsClauseType type;

        @JsonProperty("field")
        public String field;

        @JsonProperty("value")
        public String value;

        @JsonProperty("children")
        public List<QsNode> children;

        @JsonCreator
        public QsNode(@JsonProperty("type") QsClauseType type,
                @JsonProperty("field") String field,
                @JsonProperty("value") String value,
                @JsonProperty("children") List<QsNode> children) {
            this.type = type;
            this.field = field;
            this.value = value;
            this.children = children != null ? children : new ArrayList<>();
        }

        public QsNode(QsClauseType type, String field, String value) {
            this.type = type;
            this.field = field;
            this.value = value;
            this.children = new ArrayList<>();
        }

        public QsNode(QsClauseType type, List<QsNode> children) {
            this.type = type;
            this.children = children != null ? children : new ArrayList<>();
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, field, value, children);
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
            return type == qsNode.type
                    && Objects.equals(field, qsNode.field)
                    && Objects.equals(value, qsNode.value)
                    && Objects.equals(children, qsNode.children);
        }
    }

    /**
     * Field binding information extracted from DSL
     */
    public static class QsFieldBinding {
        @JsonProperty("fieldName")
        public String fieldName;

        @JsonProperty("slotIndex")
        public int slotIndex;

        @JsonCreator
        public QsFieldBinding(@JsonProperty("fieldName") String fieldName,
                @JsonProperty("slotIndex") int slotIndex) {
            this.fieldName = fieldName;
            this.slotIndex = slotIndex;
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
}
