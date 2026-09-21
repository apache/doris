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

import org.apache.doris.analysis.InvertedIndexProperties;
import org.apache.doris.catalog.Env;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;

import com.google.common.base.Strings;
import org.apache.logging.log4j.Logger;

import java.util.ArrayDeque;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public final class AnalyzerIdentityBuilder {
    private static final String PROP_MAX_NGRAM_DIFF = "max_ngram_diff";

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
            String builtinIkIdentity = resolveBuiltinIkAnalyzerIdentity(properties, preferredAnalyzer);
            if (builtinIkIdentity != null) {
                return appendOuterCharFilterIdentity(builtinIkIdentity, properties);
            }
            // For custom analyzer/normalizer, resolve to underlying config to build identity
            return appendOuterCharFilterIdentity(
                    resolveAnalyzerIdentity(preferredAnalyzer, defaultAnalyzerKey, log), properties);
        }

        if (Strings.isNullOrEmpty(parser) || parserNone.equalsIgnoreCase(parser)) {
            return defaultAnalyzerKey;
        }
        String legacyIkIdentity = resolveLegacyIkIdentity(properties, parser);
        if (legacyIkIdentity != null) {
            return appendOuterCharFilterIdentity(legacyIkIdentity, properties);
        }
        return appendOuterCharFilterIdentity(parser, properties);
    }

    private static String resolveBuiltinIkAnalyzerIdentity(
            Map<String, String> properties, String analyzer) {
        // BE dispatches canonical lowercase built-ins before custom policies.
        // Preserve the identity of case-distinct legacy policies such as "IK".
        if (!InvertedIndexProperties.INVERTED_INDEX_PARSER_IK.equals(analyzer.trim())) {
            return null;
        }
        return buildBuiltinIkIdentity("ik_max_word", properties);
    }

    private static String resolveLegacyIkIdentity(Map<String, String> properties, String parser) {
        if (!InvertedIndexProperties.INVERTED_INDEX_PARSER_IK.equalsIgnoreCase(parser)) {
            return null;
        }
        String mode = properties.get(InvertedIndexProperties.INVERTED_INDEX_PARSER_MODE_KEY);
        if (Strings.isNullOrEmpty(mode)) {
            mode = InvertedIndexProperties.INVERTED_INDEX_PARSER_SMART;
        }
        String tokenizer = normalizeBuiltinComponentName(mode, IndexPolicyTypeEnum.TOKENIZER);
        if (!"ik_smart".equals(tokenizer) && !"ik_max_word".equals(tokenizer)) {
            return null;
        }
        // Legacy IK uses the built-in tokenizer even when a named policy shadows its mode.
        return buildBuiltinIkIdentity(tokenizer, properties);
    }

    private static String buildBuiltinIkIdentity(String tokenizer, Map<String, String> properties) {
        String identity = IndexPolicyTypeEnum.ANALYZER.name() + ":tokenizer=" + tokenizer + ";";
        if (Boolean.FALSE.toString().equalsIgnoreCase(
                properties.get(InvertedIndexProperties.INVERTED_INDEX_PARSER_LOWERCASE_KEY))) {
            identity += "lower_case=false;";
        }
        return identity;
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
        String tokenizerIdentity = resolveComponentIdentity(
                properties.get(IndexPolicy.PROP_TOKENIZER), IndexPolicyTypeEnum.TOKENIZER);
        boolean lowercaseDownstream =
                "ik_smart".equals(tokenizerIdentity) || "ik_max_word".equals(tokenizerIdentity);

        StringBuilder sb = new StringBuilder();
        sb.append(type.name()).append(":");

        for (Map.Entry<String, String> entry : sortedProps.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String resolved = null;

            // For tokenizer, token_filter, char_filter - resolve recursively if needed
            if (IndexPolicy.PROP_TOKENIZER.equals(key)) {
                resolved = tokenizerIdentity;
            } else if (IndexPolicy.PROP_TOKEN_FILTER.equals(key)) {
                resolved = resolveTokenFilterIdentity(value);
            } else if (IndexPolicy.PROP_CHAR_FILTER.equals(key)) {
                resolved = resolveCharFilterIdentity(value, lowercaseDownstream);
            }
            if (!Strings.isNullOrEmpty(resolved)) {
                sb.append(key).append("=").append(resolved).append(";");
            }
        }

        return sb.toString();
    }

    /**
     * Resolve a component (tokenizer) to its identity.
     */
    private static String resolveComponentIdentity(String name, IndexPolicyTypeEnum expectedType) {
        return resolveComponentIdentity(name, expectedType, false);
    }

    private static String resolveComponentIdentity(
            String name, IndexPolicyTypeEnum expectedType, boolean lowercaseDownstream) {
        if (Strings.isNullOrEmpty(name)) {
            return "";
        }

        // Existing named policies take precedence over built-ins for upgrade compatibility.
        try {
            Env env = Env.getCurrentEnv();
            if (env != null && env.getIndexPolicyMgr() != null) {
                IndexPolicy policy = env.getIndexPolicyMgr().getPolicyByName(name);
                if (policy != null && policy.getType() == expectedType) {
                    if (policy.isInvalid()) {
                        return "invalid-policy:" + policy.getId() + ":" + policy.getName();
                    }
                    Map<String, String> props = policy.getProperties();
                    if (props != null && !props.isEmpty()) {
                        TreeMap<String, String> sortedProps = new TreeMap<>(props);
                        String type = sortedProps.get(IndexPolicy.PROP_TYPE);
                        String normalizedType = normalizeBuiltinComponentName(type, expectedType);
                        if (normalizedType != null) {
                            if ("empty".equals(normalizedType)) {
                                return "";
                            }
                            sortedProps.put(IndexPolicy.PROP_TYPE, normalizedType);
                            canonicalizeEffectiveComponentProperties(
                                    sortedProps, normalizedType, expectedType);
                            if (sortedProps.size() == 1) {
                                return normalizedType;
                            }
                        }
                        if (expectedType == IndexPolicyTypeEnum.TOKENIZER
                                && "ngram".equals(sortedProps.get(IndexPolicy.PROP_TYPE))) {
                            // This setting only limits policy creation; it does not change emitted tokens.
                            sortedProps.remove(PROP_MAX_NGRAM_DIFF);
                        }
                        if (expectedType == IndexPolicyTypeEnum.CHAR_FILTER
                                && "char_replace".equals(sortedProps.get(IndexPolicy.PROP_TYPE))) {
                            String replacement = sortedProps.getOrDefault("replacement", " ");
                            String pattern = canonicalizeCharReplacePattern(
                                    sortedProps.get("pattern"), replacement, lowercaseDownstream);
                            if (pattern.isEmpty()) {
                                return "";
                            }
                            sortedProps.put("pattern", pattern);
                            sortedProps.put("replacement", replacement);
                        }
                        if (normalizedType != null && sortedProps.size() == 1) {
                            return normalizedType;
                        }
                        return sortedProps.toString();
                    }
                }
            }
        } catch (RuntimeException e) {
            // Fall through to built-in resolution or the original name.
        }

        String normalizedName = normalizeBuiltinComponentName(name, expectedType);
        return "empty".equals(normalizedName) ? "" : normalizedName == null ? name : normalizedName;
    }

    private static void canonicalizeEffectiveComponentProperties(
            TreeMap<String, String> properties, String type, IndexPolicyTypeEnum expectedType) {
        if ("pinyin".equals(type)) {
            removeBooleanDefaults(properties, true,
                    "keep_first_letter", "keep_full_pinyin", "keep_none_chinese",
                    "keep_none_chinese_together", "keep_none_chinese_in_first_letter",
                    "lowercase", "trim_whitespace", "ignore_pinyin_offset",
                    "none_chinese_pinyin_tokenize");
            removeBooleanDefaults(properties, false,
                    "keep_separate_first_letter", "keep_joined_full_pinyin", "keep_original",
                    "keep_none_chinese_in_joined_full_pinyin", "remove_duplicated_term",
                    "fixed_pinyin_offset", "keep_separate_chinese");
            removeIntegerDefault(properties, "limit_first_letter_length", 16);
            return;
        }

        if (expectedType == IndexPolicyTypeEnum.TOKEN_FILTER) {
            if ("asciifolding".equals(type)) {
                removeBooleanDefaults(properties, false, "preserve_original");
            } else if ("word_delimiter".equals(type)) {
                removeBooleanDefaults(properties, true, "generate_word_parts", "generate_number_parts",
                        "split_on_case_change", "split_on_numerics", "stem_english_possessive");
                removeBooleanDefaults(properties, false, "catenate_words", "catenate_numbers",
                        "catenate_all", "preserve_original");
            } else if ("icu_normalizer".equals(type)) {
                canonicalizeIcuNormalizerDefaults(properties, false);
            }
            return;
        }

        if (expectedType == IndexPolicyTypeEnum.CHAR_FILTER) {
            if ("icu_normalizer".equals(type)) {
                canonicalizeIcuNormalizerDefaults(properties, true);
            }
            return;
        }

        if (expectedType != IndexPolicyTypeEnum.TOKENIZER) {
            return;
        }
        switch (type) {
            case "ngram":
                removeIntegerDefault(properties, "min_gram", 1);
                removeIntegerDefault(properties, "max_gram", 2);
                break;
            case "edge_ngram":
                removeIntegerDefault(properties, "min_gram", 1);
                removeIntegerDefault(properties, "max_gram", 2);
                break;
            case "standard":
            case "char_group":
                removeIntegerDefault(properties, "max_token_length", 255);
                break;
            case "keyword":
                removeIntegerDefault(properties, "buffer_size", 256);
                break;
            case "basic":
                removeStringDefault(properties, "extra_chars", "");
                break;
            default:
                break;
        }
    }

    private static void removeBooleanDefaults(
            TreeMap<String, String> properties, boolean defaultValue, String... keys) {
        for (String key : keys) {
            String value = properties.get(key);
            if (value == null || !("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
                continue;
            }
            boolean parsed = Boolean.parseBoolean(value);
            if (parsed == defaultValue) {
                properties.remove(key);
            } else {
                properties.put(key, Boolean.toString(parsed));
            }
        }
    }

    private static void removeIntegerDefault(
            TreeMap<String, String> properties, String key, int defaultValue) {
        String value = properties.get(key);
        if (value == null) {
            return;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed == defaultValue) {
                properties.remove(key);
            } else {
                properties.put(key, Integer.toString(parsed));
            }
        } catch (NumberFormatException e) {
            // Invalid policies keep their original identity.
        }
    }

    private static void canonicalizeIcuNormalizerDefaults(
            TreeMap<String, String> properties, boolean hasMode) {
        String name = properties.get("name");
        if (name != null) {
            String normalizedName = name.trim().toLowerCase(Locale.ROOT);
            if ("nfkc_cf".equals(normalizedName)) {
                properties.remove("name");
            } else {
                properties.put("name", normalizedName);
            }
        }
        removeStringDefault(properties, "unicode_set_filter", "");
        if (hasMode) {
            removeStringDefault(properties, "mode", "compose");
        }
    }

    private static void removeStringDefault(
            TreeMap<String, String> properties, String key, String defaultValue) {
        if (defaultValue.equals(properties.get(key))) {
            properties.remove(key);
        }
    }

    private static String normalizeBuiltinComponentName(String name, IndexPolicyTypeEnum expectedType) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        String normalizedName = name.trim().toLowerCase(Locale.ROOT);
        if ((expectedType == IndexPolicyTypeEnum.TOKENIZER
                    && IndexPolicy.BUILTIN_TOKENIZERS.contains(normalizedName))
                || (expectedType == IndexPolicyTypeEnum.TOKEN_FILTER
                    && IndexPolicy.BUILTIN_TOKEN_FILTERS.contains(normalizedName))
                || (expectedType == IndexPolicyTypeEnum.CHAR_FILTER
                    && IndexPolicy.BUILTIN_CHAR_FILTERS.contains(normalizedName))) {
            return normalizedName;
        }
        return null;
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

        for (String filterName : filters) {
            String filter = resolveComponentIdentity(filterName.trim(), IndexPolicyTypeEnum.TOKEN_FILTER);
            if (Strings.isNullOrEmpty(filter)) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(filter);
        }
        return sb.toString();
    }

    /**
     * Resolve char filter list to identity string.
     * IMPORTANT: Order is preserved because filter order is semantically significant.
     */
    private static String resolveCharFilterIdentity(String filterList) {
        return resolveCharFilterIdentity(filterList, false);
    }

    private static String resolveCharFilterIdentity(String filterList, boolean lowercaseDownstream) {
        if (Strings.isNullOrEmpty(filterList)) {
            return "";
        }

        ArrayDeque<String> identities = new ArrayDeque<>();
        String[] filters = filterList.split(",\\s*");
        // DO NOT sort - filter order is semantically significant

        for (int i = filters.length - 1; i >= 0; --i) {
            String filterName = filters[i].trim();
            String filter = resolveComponentIdentity(
                    filterName, IndexPolicyTypeEnum.CHAR_FILTER, lowercaseDownstream);
            if (Strings.isNullOrEmpty(filter)) {
                continue;
            }
            identities.addFirst(filter);
            lowercaseDownstream = isCaseFoldingCharFilter(filterName);
        }
        return String.join(",", identities);
    }

    private static boolean isCaseFoldingCharFilter(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return false;
        }

        try {
            Env env = Env.getCurrentEnv();
            if (env != null && env.getIndexPolicyMgr() != null) {
                IndexPolicy policy = env.getIndexPolicyMgr().getPolicyByName(name);
                if (policy != null && policy.getType() == IndexPolicyTypeEnum.CHAR_FILTER) {
                    if (policy.isInvalid()) {
                        return false;
                    }
                    Map<String, String> properties = policy.getProperties();
                    if (properties != null && !properties.isEmpty()) {
                        String type = normalizeBuiltinComponentName(
                                properties.get(IndexPolicy.PROP_TYPE), IndexPolicyTypeEnum.CHAR_FILTER);
                        String normalizer = properties.getOrDefault("name", "nfkc_cf").trim();
                        String unicodeSet = properties.getOrDefault("unicode_set_filter", "").trim();
                        return "icu_normalizer".equals(type)
                                && "nfkc_cf".equalsIgnoreCase(normalizer)
                                && unicodeSet.isEmpty();
                    }
                }
            }
        } catch (RuntimeException e) {
            // Fall through to built-in resolution.
        }

        return "icu_normalizer".equals(
                normalizeBuiltinComponentName(name, IndexPolicyTypeEnum.CHAR_FILTER));
    }

    private static String appendOuterCharFilterIdentity(
            String analyzerIdentity, Map<String, String> properties) {
        String type = properties.get(InvertedIndexProperties.INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE);
        String pattern = properties.get(InvertedIndexProperties.INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN);
        if (!"char_replace".equals(type) || Strings.isNullOrEmpty(pattern)) {
            return analyzerIdentity;
        }
        String replacement = properties.getOrDefault(
                InvertedIndexProperties.INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        String canonicalPattern = canonicalizeCharReplacePattern(
                pattern, replacement, isDefaultLowercaseBuiltinIkIdentity(analyzerIdentity));
        if (canonicalPattern.isEmpty()) {
            return analyzerIdentity;
        }
        return analyzerIdentity + "|outer_char_filter=char_replace:"
                + canonicalPattern.length() + ":" + canonicalPattern + ":"
                + replacement.length() + ":" + replacement + ";";
    }

    /**
     * Canonicalize the ASCII pattern to the BE filter's byte set.
     * Order, duplicate bytes, and replacements of a byte with itself do not change the stream.
     */
    private static String canonicalizeCharReplacePattern(
            String pattern, String replacement, boolean lowercaseBuiltinIk) {
        if (replacement.length() != 1) {
            return pattern;
        }
        char replacementByte = replacement.charAt(0);
        boolean[] replacedBytes = new boolean[256];
        for (int i = 0; i < pattern.length(); ++i) {
            char patternByte = pattern.charAt(i);
            if (patternByte < replacedBytes.length && patternByte != replacementByte) {
                replacedBytes[patternByte] = true;
            }
        }
        if (lowercaseBuiltinIk && replacementByte >= 'a' && replacementByte <= 'z') {
            replacedBytes[replacementByte - ('a' - 'A')] = false;
        }

        StringBuilder canonical = new StringBuilder();
        for (int i = 0; i < replacedBytes.length; ++i) {
            if (replacedBytes[i]) {
                canonical.append((char) i);
            }
        }
        return canonical.toString();
    }

    private static boolean isDefaultLowercaseBuiltinIkIdentity(String analyzerIdentity) {
        return (IndexPolicyTypeEnum.ANALYZER.name() + ":tokenizer=ik_smart;").equals(analyzerIdentity)
                || (IndexPolicyTypeEnum.ANALYZER.name() + ":tokenizer=ik_max_word;").equals(analyzerIdentity);
    }
}
