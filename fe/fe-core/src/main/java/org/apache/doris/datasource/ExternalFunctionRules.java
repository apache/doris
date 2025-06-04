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

package org.apache.doris.datasource;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.jdbc.source.JdbcFunctionPushDownRule;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * External push down rules for functions.
 * This class provides a way to define which functions can be pushed down to external data sources.
 * It supports both supported and unsupported functions in a JSON format.
 */
public class ExternalFunctionRules {
    private static final Logger LOG = LogManager.getLogger(ExternalFunctionRules.class);

    private FunctionPushDownRule functionPushDownRule;
    private FunctionRewriteRules functionRewriteRules;

    public static ExternalFunctionRules create(String datasource, String jsonRules) {
        ExternalFunctionRules rules = new ExternalFunctionRules();
        rules.functionPushDownRule = FunctionPushDownRule.create(datasource, jsonRules);
        rules.functionRewriteRules = FunctionRewriteRules.create(datasource, jsonRules);
        return rules;
    }

    public static void check(String jsonRules) throws DdlException {
        if (Strings.isNullOrEmpty(jsonRules)) {
            return;
        }
        FunctionPushDownRule.check(jsonRules);
        FunctionRewriteRules.check(jsonRules);
    }

    public FunctionPushDownRule getFunctionPushDownRule() {
        return functionPushDownRule;
    }

    public FunctionRewriteRules getFunctionRewriteRule() {
        return functionRewriteRules;
    }

    /**
     * FunctionPushDownRule is used to determine if a function can be pushed down
     */
    public static class FunctionPushDownRule {
        private final Set<String> supportedFunctions = Sets.newHashSet();
        private final Set<String> unsupportedFunctions = Sets.newHashSet();

        public static FunctionPushDownRule create(String datasource, String jsonRules) {
            FunctionPushDownRule funcRule = new FunctionPushDownRule();
            try {
                // Add default push down rules
                switch (datasource.toLowerCase()) {
                    case "mysql":
                        funcRule.unsupportedFunctions.addAll(JdbcFunctionPushDownRule.MYSQL_UNSUPPORTED_FUNCTIONS);
                        break;
                    case "clickhouse":
                        funcRule.supportedFunctions.addAll(JdbcFunctionPushDownRule.CLICKHOUSE_SUPPORTED_FUNCTIONS);
                        break;
                    case "oracle":
                        funcRule.supportedFunctions.addAll(JdbcFunctionPushDownRule.ORACLE_SUPPORTED_FUNCTIONS);
                        break;
                    default:
                        break;
                }
                if (!Strings.isNullOrEmpty(jsonRules)) {
                    // set custom rules
                    Gson gson = new Gson();
                    PushDownRules rules = gson.fromJson(jsonRules, PushDownRules.class);
                    funcRule.setCustomRules(rules);
                }
                return funcRule;
            } catch (Exception e) {
                LOG.warn("should not happen", e);
                return funcRule;
            }
        }

        public static void check(String jsonRules) throws DdlException {
            try {
                Gson gson = new Gson();
                PushDownRules rules = gson.fromJson(jsonRules, PushDownRules.class);
                if (rules == null) {
                    throw new DdlException("Push down rules cannot be null");
                }
                rules.check();
            } catch (Exception e) {
                throw new DdlException("Failed to parse push down rules: " + jsonRules, e);
            }
        }

        private void setCustomRules(PushDownRules rules) {
            if (rules != null && rules.getPushdown() != null) {
                if (rules.getPushdown().getSupported() != null) {
                    rules.getPushdown().getSupported().stream()
                            .map(String::toLowerCase)
                            .forEach(supportedFunctions::add);
                }
                if (rules.getPushdown().getUnsupported() != null) {
                    rules.getPushdown().getUnsupported().stream()
                            .map(String::toLowerCase)
                            .forEach(unsupportedFunctions::add);
                }
            }
        }

        /**
         * Checks if the function can be pushed down.
         *
         * @param functionName the name of the function to check
         * @return true if the function can be pushed down, false otherwise
         */
        public boolean canPushDown(String functionName) {
            if (supportedFunctions.isEmpty() && unsupportedFunctions.isEmpty()) {
                return false;
            }

            // If supportedFunctions is not empty, only functions in supportedFunctions can return true
            if (!supportedFunctions.isEmpty()) {
                return supportedFunctions.contains(functionName.toLowerCase());
            }

            // For functions contained in unsupportedFunctions, return false
            if (unsupportedFunctions.contains(functionName.toLowerCase())) {
                return false;
            }

            // In other cases, return true
            return true;
        }
    }

    /**
     * FunctionRewriteRule is used to rewrite function names based on provided rules.
     * It allows for mapping one function name to another.
     */
    public static class FunctionRewriteRules {
        private final Map<String, String> rewriteMap = Maps.newHashMap();

        public static FunctionRewriteRules create(String datasource, String jsonRules) {
            FunctionRewriteRules rewriteRule = new FunctionRewriteRules();
            try {
                // Add default rewrite rules
                switch (datasource.toLowerCase()) {
                    case "mysql":
                        rewriteRule.rewriteMap.putAll(JdbcFunctionPushDownRule.REPLACE_MYSQL_FUNCTIONS);
                        break;
                    case "clickhouse":
                        rewriteRule.rewriteMap.putAll(JdbcFunctionPushDownRule.REPLACE_CLICKHOUSE_FUNCTIONS);
                        break;
                    case "oracle":
                        rewriteRule.rewriteMap.putAll(JdbcFunctionPushDownRule.REPLACE_ORACLE_FUNCTIONS);
                        break;
                    default:
                        break;
                }
                if (!Strings.isNullOrEmpty(jsonRules)) {
                    // set custom rules
                    Gson gson = new Gson();
                    RewriteRules rules = gson.fromJson(jsonRules, RewriteRules.class);
                    rewriteRule.setCustomRules(rules);
                }
                return rewriteRule;
            } catch (Exception e) {
                LOG.warn("should not happen", e);
                return rewriteRule;
            }
        }

        private void setCustomRules(RewriteRules rules) {
            if (rules != null && rules.getRewrite() != null) {
                this.rewriteMap.putAll(rules.getRewrite());
            }
        }

        public String rewriteFunction(String origFuncName) {
            return rewriteMap.getOrDefault(origFuncName, origFuncName);
        }

        public static void check(String jsonRules) throws DdlException {
            try {
                Gson gson = new Gson();
                RewriteRules rules = gson.fromJson(jsonRules, RewriteRules.class);
                if (rules == null) {
                    throw new DdlException("Rewrite rules cannot be null");
                }
                rules.check();
            } catch (Exception e) {
                throw new DdlException("Failed to parse rewrite rules: " + jsonRules, e);
            }
        }
    }

    /**
     * push down rules in json format.
     * eg:
     * {
     * "pushdown": {
     * "supported": ["function1", "function2"],
     * "unsupported": ["function3", "function4"]
     * }
     * }
     */
    @Data
    public static class PushDownRules {
        private PushDown pushdown;

        @Data
        public static class PushDown {
            private List<String> supported;
            private List<String> unsupported;
        }

        public void check() {
            if (pushdown != null) {
                if (pushdown.getSupported() != null) {
                    for (String func : pushdown.getSupported()) {
                        if (Strings.isNullOrEmpty(func)) {
                            throw new IllegalArgumentException("Supported function name cannot be empty");
                        }
                    }
                }
                if (pushdown.getUnsupported() != null) {
                    for (String func : pushdown.getUnsupported()) {
                        if (Strings.isNullOrEmpty(func)) {
                            throw new IllegalArgumentException("Unsupported function name cannot be empty");
                        }
                    }
                }
            }
        }
    }

    /**
     * push down rules in json format.
     * eg:
     * {
     * "rewrite": {
     * "func1": "func2",
     * "func3": "func4"
     * }
     * }
     */
    @Data
    public static class RewriteRules {
        private Map<String, String> rewrite;

        public void check() {
            if (rewrite != null) {
                for (Map.Entry<String, String> entry : rewrite.entrySet()) {
                    String origFunc = entry.getKey();
                    String newFunc = entry.getValue();
                    if (Strings.isNullOrEmpty(origFunc) || Strings.isNullOrEmpty(newFunc)) {
                        throw new IllegalArgumentException("Function names in rewrite rules cannot be empty");
                    }
                }
            }
        }
    }
}
