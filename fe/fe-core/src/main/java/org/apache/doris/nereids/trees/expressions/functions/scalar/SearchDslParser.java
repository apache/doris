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

            // Parse the search query
            ParseTree tree = parser.search();

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
        AND,        // clause1 AND clause2
        OR,         // clause1 OR clause2
        NOT         // NOT clause
    }

    /**
     * ANTLR visitor to build QsNode AST from parse tree
     */
    private static class QsAstBuilder extends SearchParserBaseVisitor<QsNode> {
        private final Set<String> fieldNames = new HashSet<>();

        public Set<String> getFieldNames() {
            return fieldNames;
        }

        @Override
        public QsNode visitSearch(SearchParser.SearchContext ctx) {
            return visit(ctx.clause());
        }

        @Override
        public QsNode visitOrClause(SearchParser.OrClauseContext ctx) {
            if (ctx.andClause().size() == 1) {
                return visit(ctx.andClause(0));
            }

            List<QsNode> children = new ArrayList<>();
            for (SearchParser.AndClauseContext andCtx : ctx.andClause()) {
                children.add(visit(andCtx));
            }
            return new QsNode(QsClauseType.OR, children);
        }

        @Override
        public QsNode visitAndClause(SearchParser.AndClauseContext ctx) {
            if (ctx.notClause().size() == 1) {
                return visit(ctx.notClause(0));
            }

            List<QsNode> children = new ArrayList<>();
            for (SearchParser.NotClauseContext notCtx : ctx.notClause()) {
                children.add(visit(notCtx));
            }
            return new QsNode(QsClauseType.AND, children);
        }

        @Override
        public QsNode visitNotClause(SearchParser.NotClauseContext ctx) {
            if (ctx.NOT() != null) {
                QsNode child = visit(ctx.atomClause());
                List<QsNode> children = new ArrayList<>();
                children.add(child);
                return new QsNode(QsClauseType.NOT, children);
            }
            return visit(ctx.atomClause());
        }

        @Override
        public QsNode visitAtomClause(SearchParser.AtomClauseContext ctx) {
            if (ctx.clause() != null) {
                // Parenthesized clause
                return visit(ctx.clause());
            }
            return visit(ctx.fieldQuery());
        }

        @Override
        public QsNode visitFieldQuery(SearchParser.FieldQueryContext ctx) {
            String fieldName = ctx.fieldName().getText();
            if (fieldName.startsWith("\"") && fieldName.endsWith("\"")) {
                fieldName = fieldName.substring(1, fieldName.length() - 1);
            }
            fieldNames.add(fieldName);

            return visit(ctx.searchValue());
        }

        @Override
        public QsNode visitSearchValue(SearchParser.SearchValueContext ctx) {
            String fieldName = getCurrentFieldName();
            if (ctx.TERM() != null) {
                return new QsNode(QsClauseType.TERM, fieldName, ctx.TERM().getText());
            } else
                if (ctx.PREFIX() != null) {
                    return new QsNode(QsClauseType.PREFIX, fieldName, ctx.PREFIX().getText());
                } else
                    if (ctx.WILDCARD() != null) {
                        return new QsNode(QsClauseType.WILDCARD, fieldName, ctx.WILDCARD().getText());
                    } else
                        if (ctx.REGEXP() != null) {
                            String regexp = ctx.REGEXP().getText();
                            // Remove surrounding slashes
                            if (regexp.startsWith("/") && regexp.endsWith("/")) {
                                regexp = regexp.substring(1, regexp.length() - 1);
                            }
                            return new QsNode(QsClauseType.REGEXP, fieldName, regexp);
                        } else
                            if (ctx.QUOTED() != null) {
                                String quoted = ctx.QUOTED().getText();
                                // Remove surrounding quotes
                                if (quoted.startsWith("\"") && quoted.endsWith("\"")) {
                                    quoted = quoted.substring(1, quoted.length() - 1);
                                }
                                return new QsNode(QsClauseType.PHRASE, fieldName, quoted);
                            } else
                                if (ctx.rangeValue() != null) {
                                    return new QsNode(QsClauseType.RANGE, fieldName, ctx.rangeValue().getText());
                                } else
                                    if (ctx.listValue() != null) {
                                        return new QsNode(QsClauseType.LIST, fieldName, ctx.listValue().getText());
                                    } else
                                        if (ctx.anyAllValue() != null) {
                                            String anyAllText = ctx.anyAllValue().getText();
                                            String innerContent = "";

                                            // Extract content between parentheses
                                            int openParen = anyAllText.indexOf('(');
                                            int closeParen = anyAllText.lastIndexOf(')');
                                            if (openParen >= 0 && closeParen > openParen) {
                                                innerContent = anyAllText.substring(openParen + 1, closeParen).trim();
                                            }

                                            if (anyAllText.toUpperCase().startsWith("ANY(") || anyAllText.toLowerCase()
                                                    .startsWith("any(")) {
                                                return new QsNode(QsClauseType.ANY, fieldName, innerContent);
                                            } else
                                                if (anyAllText.toUpperCase().startsWith("ALL(")
                                                        || anyAllText.toLowerCase().startsWith("all(")) {
                                                    return new QsNode(QsClauseType.ALL, fieldName, innerContent);
                                                } else {
                                                    // Fallback to ANY for unknown cases
                                                    return new QsNode(QsClauseType.ANY, fieldName, innerContent);
                                                }
                                        }

            return new QsNode(QsClauseType.TERM, fieldName, ctx.getText());
        }

        private String getCurrentFieldName() {
            // This is a simplified approach - in a real implementation,
            // we'd need to track context properly
            return fieldNames.isEmpty() ? "_all" : fieldNames.iterator().next();
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
                @JsonProperty("value") String value) {
            this.type = type;
            this.field = field;
            this.value = value;
            this.children = new ArrayList<>();
        }

        public QsNode(@JsonProperty("type") QsClauseType type,
                @JsonProperty("children") List<QsNode> children) {
            this.type = type;
            this.children = children != null ? children : new ArrayList<>();
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
    }
}
