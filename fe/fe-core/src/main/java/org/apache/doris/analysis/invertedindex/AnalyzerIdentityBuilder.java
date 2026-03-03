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

package org.apache.doris.analysis.invertedindex;

import org.apache.doris.catalog.Env;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;

import com.google.common.base.Strings;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

public final class AnalyzerIdentityBuilder {
    private AnalyzerIdentityBuilder() {
    }

    public static String buildAnalyzerIdentity(
            Map<String, String> properties,
            String preferredAnalyzer,
            String parser,
            String defaultAnalyzerKey,
            String parserNone,
            Logger log) {
        if (properties == null || properties.isEmpty()) {
            return defaultAnalyzerKey;
        }

        if (!Strings.isNullOrEmpty(preferredAnalyzer)) {
            // For custom analyzer/normalizer, resolve to underlying config to build identity
            return resolveAnalyzerIdentity(preferredAnalyzer, defaultAnalyzerKey, log);
        }

        if (Strings.isNullOrEmpty(parser) || parserNone.equalsIgnoreCase(parser)) {
            return defaultAnalyzerKey;
        }
        return parser;
    }

    /**
     * Resolve analyzer/normalizer name to its underlying configuration identity.
     * Two analyzers with same underlying config (tokenizer + token_filter + char_filter)
     * will have the same identity, even if they have different names.
     */
    private static String resolveAnalyzerIdentity(String analyzerName, String defaultAnalyzerKey, Logger log) {
        if (Strings.isNullOrEmpty(analyzerName)) {
            return defaultAnalyzerKey;
        }

        // Check if it's a built-in analyzer
        if (IndexPolicy.BUILTIN_ANALYZERS.contains(analyzerName)) {
            return analyzerName;
        }

        // Check if it's a built-in normalizer
        if (IndexPolicy.BUILTIN_NORMALIZERS.contains(analyzerName)) {
            return "normalizer:" + analyzerName;
        }

        // For custom analyzer/normalizer, get underlying config from IndexPolicyMgr
        try {
            Env env = Env.getCurrentEnv();
            if (env == null || env.getIndexPolicyMgr() == null) {
                // Env not initialized - this can happen during early startup or tests
                if (log != null) {
                    log.debug("Env or IndexPolicyMgr not available, using name '{}' as identity", analyzerName);
                }
                return analyzerName;
            }

            IndexPolicy policy = env.getIndexPolicyMgr().getPolicyByName(analyzerName);
            if (policy == null) {
                // Policy not found - this is expected for custom analyzers not yet registered
                if (log != null) {
                    log.debug("Analyzer/normalizer policy not found for '{}', using name as identity", analyzerName);
                }
                return analyzerName;
            }

            Map<String, String> policyProps = policy.getProperties();
            if (policyProps == null || policyProps.isEmpty()) {
                if (log != null) {
                    log.debug("Policy '{}' has no properties, using name as identity", analyzerName);
                }
                return analyzerName;
            }

            // Build identity from underlying config using sorted keys for consistent ordering
            return buildIdentityFromPolicyProperties(policy.getType(), policyProps);
        } catch (RuntimeException e) {
            // Catch RuntimeException specifically rather than generic Exception
            if (log != null) {
                log.warn("Failed to resolve analyzer identity for '{}', using name as identity. "
                        + "This may cause incorrect duplicate detection. Error: {}",
                        analyzerName, e.getMessage());
            }
            return analyzerName;
        }
    }

    /**
     * Build identity string from policy properties.
     * Uses TreeMap to ensure consistent key ordering.
     */
    private static String buildIdentityFromPolicyProperties(IndexPolicyTypeEnum type,
            Map<String, String> properties) {
        // Use TreeMap to sort keys for consistent identity
        TreeMap<String, String> sortedProps = new TreeMap<>(properties);

        StringBuilder sb = new StringBuilder();
        sb.append(type.name()).append(":");

        for (Map.Entry<String, String> entry : sortedProps.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // For tokenizer, token_filter, char_filter - resolve recursively if needed
            if (IndexPolicy.PROP_TOKENIZER.equals(key)) {
                sb.append("tokenizer=").append(resolveComponentIdentity(value, IndexPolicyTypeEnum.TOKENIZER));
            } else if (IndexPolicy.PROP_TOKEN_FILTER.equals(key)) {
                sb.append("token_filter=").append(resolveTokenFilterIdentity(value));
            } else if (IndexPolicy.PROP_CHAR_FILTER.equals(key)) {
                sb.append("char_filter=").append(resolveCharFilterIdentity(value));
            }
            sb.append(";");
        }

        return sb.toString();
    }

    /**
     * Resolve a component (tokenizer) to its identity.
     */
    private static String resolveComponentIdentity(String name, IndexPolicyTypeEnum expectedType) {
        if (Strings.isNullOrEmpty(name)) {
            return "";
        }

        // Check if it's a built-in component
        if (expectedType == IndexPolicyTypeEnum.TOKENIZER
                && IndexPolicy.BUILTIN_TOKENIZERS.contains(name)) {
            return name;
        }

        // For custom component, get its properties
        try {
            Env env = Env.getCurrentEnv();
            if (env == null || env.getIndexPolicyMgr() == null) {
                return name;
            }

            IndexPolicy policy = env.getIndexPolicyMgr().getPolicyByName(name);
            if (policy == null || policy.getType() != expectedType) {
                return name;
            }

            Map<String, String> props = policy.getProperties();
            if (props == null || props.isEmpty()) {
                return name;
            }

            // Build identity from sorted properties
            TreeMap<String, String> sortedProps = new TreeMap<>(props);
            return sortedProps.toString();
        } catch (RuntimeException e) {
            return name;
        }
    }

    /**
     * Resolve token filter list to identity string.
     * IMPORTANT: Order is preserved because filter order is semantically significant.
     */
    private static String resolveTokenFilterIdentity(String filterList) {
        if (Strings.isNullOrEmpty(filterList)) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        String[] filters = filterList.split(",\\s*");
        // DO NOT sort - filter order is semantically significant

        for (int i = 0; i < filters.length; i++) {
            String filter = filters[i].trim();
            if (i > 0) {
                sb.append(",");
            }

            if (IndexPolicy.BUILTIN_TOKEN_FILTERS.contains(filter)) {
                sb.append(filter);
            } else {
                sb.append(resolveComponentIdentity(filter, IndexPolicyTypeEnum.TOKEN_FILTER));
            }
        }
        return sb.toString();
    }

    /**
     * Resolve char filter list to identity string.
     * IMPORTANT: Order is preserved because filter order is semantically significant.
     */
    private static String resolveCharFilterIdentity(String filterList) {
        if (Strings.isNullOrEmpty(filterList)) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        String[] filters = filterList.split(",\\s*");
        // DO NOT sort - filter order is semantically significant

        for (int i = 0; i < filters.length; i++) {
            String filter = filters[i].trim();
            if (i > 0) {
                sb.append(",");
            }

            if (IndexPolicy.BUILTIN_CHAR_FILTERS.contains(filter)) {
                sb.append(filter);
            } else {
                sb.append(resolveComponentIdentity(filter, IndexPolicyTypeEnum.CHAR_FILTER));
            }
        }
        return sb.toString();
    }
}
