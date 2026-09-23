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
import com.google.common.collect.ImmutableSet;
import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.text.UnicodeSet;
import org.apache.logging.log4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

public final class AnalyzerIdentityBuilder {
    private static final String PROP_MAX_NGRAM_DIFF = "max_ngram_diff";
    private static final String KEYWORD_TOKENIZER = "keyword";
    private static final String CHAR_REPLACE_FILTER = "char_replace";
    private static final String PROP_PATTERN = "pattern";
    private static final String PROP_REPLACEMENT = "replacement";
    // Defaults CharReplaceCharFilterFactory applies to a bare built-in reference.
    private static final String CHAR_REPLACE_DEFAULT_PATTERN = ",._";
    private static final String CHAR_REPLACE_DEFAULT_REPLACEMENT = " ";
    // Token filters that emit the same terms, offsets and provenance when applied twice in a row.
    private static final Set<String> IDEMPOTENT_TOKEN_FILTERS = ImmutableSet.of(
            "lowercase", "asciifolding", "icu_normalizer");
    // Same separator BE uses between bracketed list entries.
    private static final Pattern ENTRY_SEPARATOR = Pattern.compile("(?<=\\])\\s*,\\s*(?=\\[)");
    private static final Set<String> WORD_DELIMITER_TYPES = ImmutableSet.of(
            "LOWER", "UPPER", "ALPHA", "DIGIT", "ALPHANUM", "SUBWORD_DELIM");
    private static final Set<String> CHAR_GROUP_TYPES = ImmutableSet.of(
            "letter", "digit", "whitespace", "punctuation", "symbol", "cjk");

    private AnalyzerIdentityBuilder() {
    }

    /**
     * Case-folding context of a char filter: the bytes that filters between it and the downstream
     * fold rewrite, plus the unicode_set_filter the fold is restricted to (null for every code point).
     */
    private static final class FoldContext {
        private static final UnicodeSet NON_STARTERS = new UnicodeSet("[:^ccc=0:]").freeze();

        private final boolean[] blockedBytes = new boolean[256];
        private final UnicodeSet foldSet;

        private FoldContext(UnicodeSet foldSet) {
            this.foldSet = foldSet;
        }

        private static FoldContext unfiltered() {
            return new FoldContext(null);
        }

        private void block(boolean[] sourceBytes) {
            for (int i = 0; i < blockedBytes.length; ++i) {
                blockedBytes[i] |= sourceBytes[i];
            }
        }

        /** Whether the fold turns the upper-case ASCII byte into the lower-case one wherever it appears. */
        private boolean foldsByte(int upperByte, int lowerByte) {
            if (blockedBytes[upperByte] || blockedBytes[lowerByte]) {
                return false;
            }
            if (foldSet == null) {
                return true;
            }
            // A filtered normalizer handles each in-set span on its own, so the folded byte must
            // stay in its span, or the set must hold no combining mark that could compose with it.
            return foldSet.contains(upperByte)
                    && (foldSet.contains(lowerByte) || !foldSet.containsSome(NON_STARTERS));
        }
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
                return appendOuterCharFilterIdentity(
                        builtinIkIdentity, properties, builtinIkFoldContext(builtinIkIdentity));
            }
            // For custom analyzer/normalizer, resolve to underlying config to build identity
            return appendOuterCharFilterIdentity(
                    resolveAnalyzerIdentity(preferredAnalyzer, defaultAnalyzerKey, log), properties,
                    customAnalyzerFoldContext(preferredAnalyzer));
        }

        if (Strings.isNullOrEmpty(parser) || parserNone.equalsIgnoreCase(parser)) {
            return defaultAnalyzerKey;
        }
        String legacyIkIdentity = resolveLegacyIkIdentity(properties, parser);
        if (legacyIkIdentity != null) {
            return appendOuterCharFilterIdentity(
                    legacyIkIdentity, properties, builtinIkFoldContext(legacyIkIdentity));
        }
        return appendOuterCharFilterIdentity(parser, properties, null);
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

        if (isBuiltinNormalizerBinding(analyzerName)) {
            return builtinNormalizerIdentity(analyzerName);
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

    /** Whether BE builds the built-in normalizer for this name; an exact legacy policy shadows it. */
    private static boolean isBuiltinNormalizerBinding(String name) {
        try {
            Env env = Env.getCurrentEnv();
            if (env != null && env.getIndexPolicyMgr() != null) {
                return env.getIndexPolicyMgr().getTopLevelBuiltin(
                        name, IndexPolicy.BUILTIN_NORMALIZERS) != null;
            }
        } catch (RuntimeException e) {
            // Fall through to the name-only answer.
        }
        return IndexPolicy.BUILTIN_NORMALIZERS.contains(
                Strings.nullToEmpty(name).trim().toLowerCase(Locale.ROOT));
    }

    /**
     * BE builds a built-in normalizer as the keyword tokenizer plus the built-in token filter of
     * the canonical name, so it shares the identity of that custom pipeline.
     */
    private static String builtinNormalizerIdentity(String name) {
        return buildIdentityFromPolicyProperties(IndexPolicyTypeEnum.NORMALIZER,
                Map.of(IndexPolicy.PROP_TOKEN_FILTER, name.trim().toLowerCase(Locale.ROOT)));
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
        FoldContext downstreamFold = foldsAsciiCaseAfterCharFilters(type, properties, tokenizerIdentity);

        IndexPolicyTypeEnum identityType = type;
        if (type == IndexPolicyTypeEnum.NORMALIZER) {
            // BE's CustomNormalizer is the keyword tokenizer plus the configured char and token
            // filters, so it emits what the equivalent analyzer emits and shares its identity.
            identityType = IndexPolicyTypeEnum.ANALYZER;
            tokenizerIdentity = KEYWORD_TOKENIZER;
            sortedProps.put(IndexPolicy.PROP_TOKENIZER, KEYWORD_TOKENIZER);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(identityType.name()).append(":");

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
                resolved = resolveCharFilterIdentity(value, downstreamFold);
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
        return resolveComponentIdentity(name, expectedType, null);
    }

    /** {@code fold} is the case-folding context of a char filter, or null without a downstream fold. */
    private static String resolveComponentIdentity(
            String name, IndexPolicyTypeEnum expectedType, FoldContext fold) {
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
                                && CHAR_REPLACE_FILTER.equals(sortedProps.get(IndexPolicy.PROP_TYPE))) {
                            String replacement = sortedProps.getOrDefault(
                                    PROP_REPLACEMENT, CHAR_REPLACE_DEFAULT_REPLACEMENT);
                            String pattern = canonicalizeCharReplacePattern(
                                    sortedProps.getOrDefault(PROP_PATTERN, CHAR_REPLACE_DEFAULT_PATTERN),
                                    replacement, fold);
                            if (pattern.isEmpty()) {
                                return "";
                            }
                            if (isCharReplaceDefault(pattern, replacement, fold)) {
                                // Restating the factory defaults is the bare built-in reference.
                                sortedProps.remove(PROP_PATTERN);
                                sortedProps.remove(PROP_REPLACEMENT);
                            } else {
                                sortedProps.put(PROP_PATTERN, pattern);
                                sortedProps.put(PROP_REPLACEMENT, replacement);
                            }
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

    /** Whether this canonical char_replace configuration is what a bare built-in reference gets. */
    private static boolean isCharReplaceDefault(String pattern, String replacement, FoldContext fold) {
        return CHAR_REPLACE_DEFAULT_REPLACEMENT.equals(replacement)
                && canonicalizeCharReplacePattern(
                        CHAR_REPLACE_DEFAULT_PATTERN, replacement, fold).equals(pattern);
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
            canonicalizePinyinDependencies(properties, expectedType);
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
                canonicalizeWordSet(properties, "protected_words");
                canonicalizeTypeTable(properties);
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
            case "edge_ngram":
                removeIntegerDefault(properties, "min_gram", 1);
                removeIntegerDefault(properties, "max_gram", 2);
                canonicalizeWordSet(properties, "token_chars");
                canonicalizeCustomTokenChars(properties);
                break;
            case "standard":
                removeIntegerDefault(properties, "max_token_length", 255);
                break;
            case "char_group":
                removeIntegerDefault(properties, "max_token_length", 255);
                canonicalizeTokenizeOnChars(properties);
                break;
            case "keyword":
                // BE only range-checks buffer_size; the emitted term is always capped by a constant.
                properties.remove("buffer_size");
                break;
            case "basic":
                canonicalizeBasicExtraChars(properties);
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
        String filter = properties.get("unicode_set_filter");
        if (filter != null && filter.isEmpty()) {
            // BE treats an explicit empty string like an absent filter.
            properties.remove("unicode_set_filter");
        } else if (filter != null) {
            try {
                UnicodeSet unicodeSet = new UnicodeSet(filter);
                if (unicodeSet.isEmpty()) {
                    properties.remove("unicode_set_filter");
                } else {
                    properties.put("unicode_set_filter", unicodeSet.toPattern(false));
                }
            } catch (IllegalArgumentException e) {
                // Invalid policies keep their original identity.
            }
        }
        if (hasMode) {
            canonicalizeIcuNormalizerMode(properties);
        }
    }

    private static void canonicalizeIcuNormalizerMode(TreeMap<String, String> properties) {
        removeStringDefault(properties, "mode", "compose");
        if (!"decompose".equals(properties.get("mode"))) {
            return;
        }
        // BE ignores mode for nfd/nfkd, and nfc/nfkc in decompose mode are the same ICU instances.
        String name = properties.get("name");
        if ("nfc".equals(name) || "nfd".equals(name)) {
            properties.put("name", "nfd");
            properties.remove("mode");
        } else if ("nfkc".equals(name) || "nfkd".equals(name)) {
            properties.put("name", "nfkd");
            properties.remove("mode");
        }
    }

    // BE reads these settings as unordered sets of trimmed, non-empty words.
    private static void canonicalizeWordSet(TreeMap<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null) {
            return;
        }
        TreeSet<String> words = new TreeSet<>();
        for (String word : value.split(",")) {
            String trimmed = trimAsciiWhitespace(word);
            if (!trimmed.isEmpty()) {
                words.add(trimmed);
            }
        }
        if (words.isEmpty()) {
            properties.remove(key);
        } else {
            properties.put(key, String.join(",", words));
        }
    }

    // BE matches custom token characters as a code point set ORed with the named classes.
    private static void canonicalizeCustomTokenChars(TreeMap<String, String> properties) {
        String value = properties.get("custom_token_chars");
        if (value == null) {
            return;
        }
        String tokenChars = properties.getOrDefault("token_chars", "");
        Set<String> classes = new TreeSet<>(List.of(tokenChars.split(",")));
        StringBuilder canonical = new StringBuilder();
        value.codePoints().distinct().sorted()
                .filter(codePoint -> !isCoveredByAsciiClass(codePoint, classes))
                .forEach(canonical::appendCodePoint);
        if (canonical.length() == 0 && !value.isEmpty() && classes.remove("custom")) {
            properties.remove("custom_token_chars");
            properties.put("token_chars", String.join(",", classes));
            return;
        }
        properties.put("custom_token_chars", canonical.toString());
    }

    // BE collects tokenize_on_chars entries into sets and checks the categories before the literals.
    private static void canonicalizeTokenizeOnChars(TreeMap<String, String> properties) {
        List<String> entries = parseEntryList(properties.get("tokenize_on_chars"));
        if (entries == null) {
            return;
        }
        TreeSet<String> canonical = new TreeSet<>(entries);
        Set<String> classes = new TreeSet<>(canonical);
        classes.retainAll(CHAR_GROUP_TYPES);
        canonical.removeIf(entry -> entry.indexOf('\\') < 0
                && entry.codePointCount(0, entry.length()) == 1
                && isCoveredByAsciiClass(entry.codePointAt(0), classes));
        putEntryList(properties, "tokenize_on_chars", canonical);
    }

    /**
     * Whether a named character class of the ngram or char_group tokenizer already matches this
     * code point. Only ASCII is judged: its categories never change between the ICU versions FE
     * and BE link against, and the class predicates agree there.
     */
    private static boolean isCoveredByAsciiClass(int codePoint, Set<String> classes) {
        if (codePoint >= 128) {
            return false;
        }
        int type = UCharacter.getType(codePoint);
        for (String name : classes) {
            switch (name) {
                case "letter":
                    if (UCharacter.isLetter(codePoint)) {
                        return true;
                    }
                    break;
                case "digit":
                    if (UCharacter.isDigit(codePoint)) {
                        return true;
                    }
                    break;
                case "whitespace":
                    if (UCharacter.isWhitespace(codePoint)) {
                        return true;
                    }
                    break;
                case "punctuation":
                    if (type == UCharacter.START_PUNCTUATION || type == UCharacter.END_PUNCTUATION
                            || type == UCharacter.OTHER_PUNCTUATION || type == UCharacter.CONNECTOR_PUNCTUATION
                            || type == UCharacter.DASH_PUNCTUATION || type == UCharacter.INITIAL_PUNCTUATION
                            || type == UCharacter.FINAL_PUNCTUATION) {
                        return true;
                    }
                    break;
                case "symbol":
                    if (type == UCharacter.CURRENCY_SYMBOL || type == UCharacter.MATH_SYMBOL
                            || type == UCharacter.OTHER_SYMBOL || type == UCharacter.MODIFIER_SYMBOL) {
                        return true;
                    }
                    break;
                default:
                    break;
            }
        }
        return false;
    }

    // BE builds a per-character type map where a later rule for the same character wins.
    private static void canonicalizeTypeTable(TreeMap<String, String> properties) {
        List<String> rules = parseEntryList(properties.get("type_table"));
        if (rules == null) {
            return;
        }
        TreeMap<Integer, String> types = new TreeMap<>();
        for (String rule : rules) {
            int arrow = rule.lastIndexOf("=>");
            if (arrow < 0 || rule.indexOf('\n') >= 0 || rule.indexOf('\r') >= 0) {
                return;
            }
            String character = trimAsciiWhitespace(rule.substring(0, arrow));
            String type = trimAsciiWhitespace(rule.substring(arrow + 2));
            // Escaped characters keep the original identity rather than reproducing BE unescaping.
            if (character.indexOf('\\') >= 0 || character.codePointCount(0, character.length()) != 1
                    || !WORD_DELIMITER_TYPES.contains(type)) {
                return;
            }
            types.put(character.codePointAt(0), type);
        }
        // An explicit table uses BE's generated classification, including when all rules restate it.
        TreeMap<Integer, String> effectiveTypes = new TreeMap<>(types);
        effectiveTypes.entrySet().removeIf(
                entry -> entry.getValue().equals(defaultWordDelimiterType(entry.getKey())));
        if (effectiveTypes.isEmpty() && !types.isEmpty()) {
            properties.put("type_table", "");
            return;
        }
        List<String> canonicalRules = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : effectiveTypes.entrySet()) {
            canonicalRules.add(new String(Character.toChars(entry.getKey())) + "=>" + entry.getValue());
        }
        putEntryList(properties, "type_table", canonicalRules);
    }

    /** BE's u_charType classification of an ASCII code point, or null for anything else. */
    private static String defaultWordDelimiterType(int codePoint) {
        if (codePoint >= 128) {
            return null;
        }
        switch (UCharacter.getType(codePoint)) {
            case UCharacter.UPPERCASE_LETTER:
                return "UPPER";
            case UCharacter.LOWERCASE_LETTER:
                return "LOWER";
            case UCharacter.DECIMAL_DIGIT_NUMBER:
                return "DIGIT";
            default:
                return "SUBWORD_DELIM";
        }
    }

    /** Parse a bracketed entry list as BE does, or return null for a malformed list. */
    private static List<String> parseEntryList(String value) {
        if (value == null) {
            return null;
        }
        List<String> entries = new ArrayList<>();
        String trimmed = trimAsciiWhitespace(value);
        if (trimmed.isEmpty()) {
            return entries;
        }
        for (String item : ENTRY_SEPARATOR.split(trimmed)) {
            String entry = trimAsciiWhitespace(item);
            if (entry.length() < 2 || entry.charAt(0) != '[' || entry.charAt(entry.length() - 1) != ']') {
                return null;
            }
            String content = entry.substring(1, entry.length() - 1);
            if (!content.isEmpty()) {
                entries.add(content);
            }
        }
        return entries;
    }

    private static void putEntryList(TreeMap<String, String> properties, String key, Collection<String> entries) {
        if (entries.isEmpty()) {
            properties.remove(key);
            return;
        }
        StringBuilder canonical = new StringBuilder();
        for (String entry : entries) {
            if (canonical.length() > 0) {
                canonical.append(",");
            }
            canonical.append("[").append(entry).append("]");
        }
        properties.put(key, canonical.toString());
    }

    // Trim the same ASCII whitespace that BE trims.
    private static String trimAsciiWhitespace(String value) {
        int begin = 0;
        int end = value.length();
        while (begin < end && isAsciiWhitespace(value.charAt(begin))) {
            ++begin;
        }
        while (end > begin && isAsciiWhitespace(value.charAt(end - 1))) {
            --end;
        }
        return value.substring(begin, end);
    }

    private static boolean isAsciiWhitespace(char value) {
        return value == ' ' || (value >= '\t' && value <= '\r');
    }

    // BE consumes an ASCII alphanumeric run before it consults extra_chars.
    private static void canonicalizeBasicExtraChars(TreeMap<String, String> properties) {
        String extraChars = properties.get("extra_chars");
        if (extraChars == null) {
            return;
        }
        boolean[] present = new boolean[128];
        for (int i = 0; i < extraChars.length(); ++i) {
            char value = extraChars.charAt(i);
            if (value >= present.length) {
                return;
            }
            boolean alphanumeric = (value >= '0' && value <= '9') || (value >= 'A' && value <= 'Z')
                    || (value >= 'a' && value <= 'z');
            present[value] = !alphanumeric;
        }
        StringBuilder canonical = new StringBuilder();
        for (int i = 0; i < present.length; ++i) {
            if (present[i]) {
                canonical.append((char) i);
            }
        }
        if (canonical.length() == 0) {
            properties.remove("extra_chars");
        } else {
            properties.put("extra_chars", canonical.toString());
        }
    }

    private static void canonicalizePinyinDependencies(
            TreeMap<String, String> properties, IndexPolicyTypeEnum expectedType) {
        Boolean keepFirstLetter = effectiveBoolean(properties, "keep_first_letter", true);
        Boolean keepFullPinyin = effectiveBoolean(properties, "keep_full_pinyin", true);
        Boolean keepSeparateFirstLetter = effectiveBoolean(properties, "keep_separate_first_letter", false);
        Boolean keepOriginal = effectiveBoolean(properties, "keep_original", false);
        Boolean keepNoneChinese = effectiveBoolean(properties, "keep_none_chinese", true);
        Boolean keepNoneChineseTogether = effectiveBoolean(properties, "keep_none_chinese_together", true);
        Boolean noneChinesePinyinTokenize = effectiveBoolean(properties, "none_chinese_pinyin_tokenize", true);
        Boolean ignorePinyinOffset = effectiveBoolean(properties, "ignore_pinyin_offset", true);
        Boolean keepJoinedFullPinyin = effectiveBoolean(properties, "keep_joined_full_pinyin", false);
        // Only the pinyin tokenizer also consults keep_none_chinese_in_joined_full_pinyin, when no
        // other setting settles whether it emits an untokenized ASCII buffer.
        boolean tokenizerReadsJoinedSetting = expectedType == IndexPolicyTypeEnum.TOKENIZER
                && !Boolean.FALSE.equals(keepNoneChinese)
                && !Boolean.FALSE.equals(keepNoneChineseTogether)
                && !Boolean.TRUE.equals(noneChinesePinyinTokenize)
                && !Boolean.TRUE.equals(keepFirstLetter)
                && !Boolean.TRUE.equals(keepSeparateFirstLetter)
                && !Boolean.TRUE.equals(keepFullPinyin);

        // The tokenizer only trims its candidates, and without the original every candidate is
        // pinyin or ASCII alphanumerics; the token filter also trims the incoming token.
        if (expectedType == IndexPolicyTypeEnum.TOKENIZER && Boolean.FALSE.equals(keepOriginal)) {
            properties.remove("trim_whitespace");
        }

        // Without per-character outputs, all remaining candidates have position 1.
        // Deduplicating by term or by term and position has the same effect.
        if (Boolean.FALSE.equals(keepNoneChinese)
                && Boolean.FALSE.equals(keepFullPinyin)
                && Boolean.FALSE.equals(keepSeparateFirstLetter)
                && Boolean.FALSE.equals(effectiveBoolean(properties, "keep_separate_chinese", false))) {
            properties.remove("remove_duplicated_term");
        }

        if (Boolean.FALSE.equals(keepFirstLetter)) {
            properties.remove("limit_first_letter_length");
            properties.remove("keep_none_chinese_in_first_letter");
        }

        if (Boolean.FALSE.equals(keepNoneChinese)) {
            properties.remove("keep_none_chinese_together");
            properties.remove("none_chinese_pinyin_tokenize");
        } else if (Boolean.TRUE.equals(keepNoneChinese) && Boolean.FALSE.equals(keepNoneChineseTogether)) {
            // BE emits each ASCII letter on its own here and never tokenizes an ASCII buffer.
            properties.remove("none_chinese_pinyin_tokenize");
        }

        if (Boolean.TRUE.equals(ignorePinyinOffset)
                || Boolean.FALSE.equals(keepNoneChinese)
                || Boolean.FALSE.equals(keepNoneChineseTogether)
                || Boolean.FALSE.equals(noneChinesePinyinTokenize)) {
            properties.remove("fixed_pinyin_offset");
        }

        // The joined full pinyin buffer is only emitted behind keep_joined_full_pinyin.
        if (Boolean.FALSE.equals(keepJoinedFullPinyin) && !tokenizerReadsJoinedSetting) {
            properties.remove("keep_none_chinese_in_joined_full_pinyin");
        }

        // The ASCII alphabet tokenizer and pinyin dictionary already emit lowercase candidates.
        // The token filter keeps this setting because its fallback can carry the source case.
        if (expectedType == IndexPolicyTypeEnum.TOKENIZER
                && Boolean.FALSE.equals(keepFirstLetter)
                && Boolean.FALSE.equals(keepOriginal)
                && Boolean.FALSE.equals(keepJoinedFullPinyin)
                && (Boolean.FALSE.equals(keepNoneChinese)
                        || (Boolean.TRUE.equals(keepNoneChinese)
                                && Boolean.TRUE.equals(keepNoneChineseTogether)
                                && Boolean.TRUE.equals(noneChinesePinyinTokenize)))) {
            properties.remove("lowercase");
        }
    }

    private static Boolean effectiveBoolean(
            TreeMap<String, String> properties, String key, boolean defaultValue) {
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        return null;
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

        String previous = null;
        for (String filterName : filters) {
            String filter = resolveComponentIdentity(filterName.trim(), IndexPolicyTypeEnum.TOKEN_FILTER);
            if (Strings.isNullOrEmpty(filter)) {
                continue;
            }
            // Repeating an idempotent filter leaves the terms, offsets and provenance unchanged.
            if (filter.equals(previous) && IDEMPOTENT_TOKEN_FILTERS.contains(filter)) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(filter);
            previous = filter;
        }
        return sb.toString();
    }

    /**
     * Resolve char filter list to identity string.
     * IMPORTANT: Order is preserved because filter order is semantically significant.
     */
    private static String resolveCharFilterIdentity(String filterList) {
        return resolveCharFilterIdentity(filterList, null);
    }

    private static String resolveCharFilterIdentity(String filterList, FoldContext downstreamFold) {
        ArrayDeque<String> identities = new ArrayDeque<>();
        walkCharFilters(filterList, downstreamFold, identities);
        return String.join(",", identities);
    }

    /**
     * Resolve the chain from its last filter to its first, collecting identities, and return the
     * case-folding context that a filter placed in front of the chain would run in.
     */
    private static FoldContext walkCharFilters(
            String filterList, FoldContext downstreamFold, Deque<String> identities) {
        FoldContext fold = downstreamFold;
        if (Strings.isNullOrEmpty(filterList)) {
            return fold;
        }

        String[] filters = filterList.split(",\\s*");
        // DO NOT sort - filter order is semantically significant

        for (int i = filters.length - 1; i >= 0; --i) {
            String filterName = filters[i].trim();
            String filter = resolveComponentIdentity(filterName, IndexPolicyTypeEnum.CHAR_FILTER, fold);
            if (Strings.isNullOrEmpty(filter)) {
                continue;
            }
            // Repeating a char_replace filter rewrites the same bytes to the same byte again.
            if (!filter.equals(identities.peekFirst()) || !isIdempotentCharFilter(filterName)) {
                identities.addFirst(filter);
            }
            fold = foldContextBefore(filterName, fold);
        }
        return fold;
    }

    /**
     * Context for the filter that runs before this one: a case fold starts a fresh context, a
     * char_replace filter adds the bytes it rewrites, and any other filter ends the context.
     */
    private static FoldContext foldContextBefore(String filterName, FoldContext fold) {
        FoldContext caseFold = caseFoldingCharFilterContext(filterName);
        if (caseFold != null) {
            return caseFold;
        }
        if (fold == null) {
            return null;
        }
        boolean[] sourceBytes = charReplaceSourceBytes(filterName);
        if (sourceBytes == null) {
            return null;
        }
        fold.block(sourceBytes);
        return fold;
    }

    /**
     * Whether the filter is a usable char_replace, which replaces each pattern byte with the same
     * single byte and so leaves the stream unchanged when it runs again.
     */
    private static boolean isIdempotentCharFilter(String filterName) {
        return charReplaceSourceBytes(filterName) != null;
    }

    /**
     * Bytes a char_replace filter rewrites, or null for any other filter. A bare built-in reference
     * is instantiated with the factory defaults.
     */
    private static boolean[] charReplaceSourceBytes(String filterName) {
        String pattern = CHAR_REPLACE_DEFAULT_PATTERN;
        String replacement = CHAR_REPLACE_DEFAULT_REPLACEMENT;
        IndexPolicy policy = findPolicy(filterName, IndexPolicyTypeEnum.CHAR_FILTER);
        if (policy != null) {
            if (policy.isInvalid() || policy.getProperties() == null) {
                return null;
            }
            Map<String, String> properties = policy.getProperties();
            String type = normalizeBuiltinComponentName(
                    properties.get(IndexPolicy.PROP_TYPE), IndexPolicyTypeEnum.CHAR_FILTER);
            if (!CHAR_REPLACE_FILTER.equals(type)) {
                return null;
            }
            pattern = properties.getOrDefault(PROP_PATTERN, CHAR_REPLACE_DEFAULT_PATTERN);
            replacement = properties.getOrDefault(PROP_REPLACEMENT, CHAR_REPLACE_DEFAULT_REPLACEMENT);
        } else if (!CHAR_REPLACE_FILTER.equals(
                normalizeBuiltinComponentName(filterName, IndexPolicyTypeEnum.CHAR_FILTER))) {
            return null;
        }
        // Replacing the single replacement byte with itself leaves the stream unchanged.
        int replacementByte = replacement.length() == 1 && replacement.charAt(0) < 128 ? replacement.charAt(0) : -1;
        boolean[] sourceBytes = new boolean[256];
        for (int i = 0; i < pattern.length(); ++i) {
            char patternByte = pattern.charAt(i);
            if (patternByte < sourceBytes.length && patternByte != replacementByte) {
                sourceBytes[patternByte] = true;
            }
        }
        return sourceBytes;
    }

    /** The named policy when one exists with the expected type, or null. */
    private static IndexPolicy findPolicy(String name, IndexPolicyTypeEnum expectedType) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        try {
            Env env = Env.getCurrentEnv();
            if (env != null && env.getIndexPolicyMgr() != null) {
                IndexPolicy policy = env.getIndexPolicyMgr().getPolicyByName(name);
                if (policy != null && policy.getType() == expectedType) {
                    return policy;
                }
            }
        } catch (RuntimeException e) {
            // Treat lookup failures as an unknown policy.
        }
        return null;
    }

    /** Fold context started by a named or built-in case-folding char filter, or null for any other filter. */
    private static FoldContext caseFoldingCharFilterContext(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }

        try {
            Env env = Env.getCurrentEnv();
            if (env != null && env.getIndexPolicyMgr() != null) {
                IndexPolicy policy = env.getIndexPolicyMgr().getPolicyByName(name);
                if (policy != null && policy.getType() == IndexPolicyTypeEnum.CHAR_FILTER) {
                    if (policy.isInvalid()) {
                        return null;
                    }
                    Map<String, String> properties = policy.getProperties();
                    if (properties != null && !properties.isEmpty()) {
                        String type = normalizeBuiltinComponentName(
                                properties.get(IndexPolicy.PROP_TYPE), IndexPolicyTypeEnum.CHAR_FILTER);
                        return "icu_normalizer".equals(type) ? icuNormalizerFoldContext(properties) : null;
                    }
                }
            }
        } catch (RuntimeException e) {
            // Fall through to built-in resolution.
        }

        return "icu_normalizer".equals(normalizeBuiltinComponentName(name, IndexPolicyTypeEnum.CHAR_FILTER))
                ? FoldContext.unfiltered() : null;
    }

    /**
     * Fold context of an icu_normalizer component: the default nfkc_cf form folds case over every
     * code point, or only inside a parsable non-empty unicode_set_filter. Null for other forms.
     */
    private static FoldContext icuNormalizerFoldContext(Map<String, String> properties) {
        if (!"nfkc_cf".equals(icuNormalizerName(properties))) {
            return null;
        }
        String filter = properties.get("unicode_set_filter");
        if (filter == null || filter.isEmpty()) {
            return FoldContext.unfiltered();
        }
        try {
            UnicodeSet unicodeSet = new UnicodeSet(filter);
            return unicodeSet.isEmpty() ? FoldContext.unfiltered() : new FoldContext(unicodeSet.freeze());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** Whether an icu_normalizer component leaves ASCII letters as they are. */
    private static boolean isAsciiCaseTransparentIcuNormalizer(Map<String, String> properties) {
        String name = icuNormalizerName(properties);
        return "nfc".equals(name) || "nfd".equals(name) || "nfkc".equals(name) || "nfkd".equals(name);
    }

    private static String icuNormalizerName(Map<String, String> properties) {
        return properties.getOrDefault("name", "nfkc_cf").trim().toLowerCase(Locale.ROOT);
    }

    /** The outer char filter runs before everything else, so it takes the analyzer's fold context. */
    private static String appendOuterCharFilterIdentity(
            String analyzerIdentity, Map<String, String> properties, FoldContext fold) {
        String type = properties.get(InvertedIndexProperties.INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE);
        String pattern = properties.get(InvertedIndexProperties.INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN);
        if (!"char_replace".equals(type) || Strings.isNullOrEmpty(pattern)) {
            return analyzerIdentity;
        }
        String replacement = properties.getOrDefault(
                InvertedIndexProperties.INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        String canonicalPattern = canonicalizeCharReplacePattern(pattern, replacement, fold);
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
            String pattern, String replacement, FoldContext fold) {
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
        if (fold != null && replacementByte >= 'a' && replacementByte <= 'z') {
            // The downstream fold maps the upper-case byte to the replacement anyway.
            int upperByte = replacementByte - ('a' - 'A');
            if (fold.foldsByte(upperByte, replacementByte)) {
                replacedBytes[upperByte] = false;
            }
        } else if (fold != null && replacementByte >= 'A' && replacementByte <= 'Z') {
            // The downstream fold maps the replacement back to the lower-case byte it replaced.
            int lowerByte = replacementByte + ('a' - 'A');
            if (fold.foldsByte(replacementByte, lowerByte)) {
                replacedBytes[lowerByte] = false;
            }
        }

        StringBuilder canonical = new StringBuilder();
        for (int i = 0; i < replacedBytes.length; ++i) {
            if (replacedBytes[i]) {
                canonical.append((char) i);
            }
        }
        return canonical.toString();
    }

    private static FoldContext builtinIkFoldContext(String analyzerIdentity) {
        return isDefaultLowercaseBuiltinIkIdentity(analyzerIdentity) ? FoldContext.unfiltered() : null;
    }

    private static boolean isDefaultLowercaseBuiltinIkIdentity(String analyzerIdentity) {
        return (IndexPolicyTypeEnum.ANALYZER.name() + ":tokenizer=ik_smart;").equals(analyzerIdentity)
                || (IndexPolicyTypeEnum.ANALYZER.name() + ":tokenizer=ik_max_word;").equals(analyzerIdentity);
    }

    /**
     * Fold context for the outer char filter of a custom analyzer or normalizer, which BE applies
     * before the policy's own char filters. Unknown or unresolvable policies get no context.
     */
    private static FoldContext customAnalyzerFoldContext(String analyzerName) {
        if (IndexPolicy.BUILTIN_ANALYZERS.contains(analyzerName)) {
            return null;
        }
        if (isBuiltinNormalizerBinding(analyzerName)) {
            // The built-in normalizer lowercases keyword tokens without char filters of its own.
            return FoldContext.unfiltered();
        }
        IndexPolicy policy = findPolicy(analyzerName, IndexPolicyTypeEnum.ANALYZER);
        if (policy == null) {
            policy = findPolicy(analyzerName, IndexPolicyTypeEnum.NORMALIZER);
        }
        if (policy == null || policy.isInvalid() || policy.getProperties() == null
                || policy.getProperties().isEmpty()) {
            return null;
        }
        Map<String, String> properties = policy.getProperties();
        try {
            String tokenizerIdentity = resolveComponentIdentity(
                    properties.get(IndexPolicy.PROP_TOKENIZER), IndexPolicyTypeEnum.TOKENIZER);
            return walkCharFilters(properties.get(IndexPolicy.PROP_CHAR_FILTER),
                    foldsAsciiCaseAfterCharFilters(policy.getType(), properties, tokenizerIdentity),
                    new ArrayDeque<>());
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * The fold the tokenizer and token filters apply to ASCII letters, so a char filter that only
     * lowercases such a letter cannot change the output, or null when they keep case.
     */
    private static FoldContext foldsAsciiCaseAfterCharFilters(
            IndexPolicyTypeEnum type, Map<String, String> properties, String tokenizerIdentity) {
        if (type == IndexPolicyTypeEnum.NORMALIZER) {
            // A normalizer always tokenizes with keyword, which is case transparent.
            return tokenFiltersFoldAsciiCase(properties.get(IndexPolicy.PROP_TOKEN_FILTER));
        }
        if ("ik_smart".equals(tokenizerIdentity) || "ik_max_word".equals(tokenizerIdentity)) {
            return FoldContext.unfiltered();
        }
        return isCaseTransparentTokenizer(properties.get(IndexPolicy.PROP_TOKENIZER))
                ? tokenFiltersFoldAsciiCase(properties.get(IndexPolicy.PROP_TOKEN_FILTER)) : null;
    }

    /** Whether the tokenizer splits and emits ASCII letters the same way regardless of their case. */
    private static boolean isCaseTransparentTokenizer(String name) {
        TreeMap<String, String> settings = resolveComponentSettings(name, IndexPolicyTypeEnum.TOKENIZER);
        if (settings == null) {
            return false;
        }
        String type = settings.get(IndexPolicy.PROP_TYPE);
        // Judge the same canonical settings the tokenizer identity is built from.
        canonicalizeEffectiveComponentProperties(settings, type, IndexPolicyTypeEnum.TOKENIZER);
        switch (type) {
            case "standard":
            case "keyword":
            case "icu":
            case "basic":
                return true;
            case "ngram":
            case "edge_ngram":
                return !settings.containsKey("custom_token_chars");
            case "char_group":
                return tokenizeOnCharsIgnoreAsciiLetters(settings.get("tokenize_on_chars"));
            default:
                return false;
        }
    }

    /** Settings of a named or built-in component with a canonical type, or null when unknown. */
    private static TreeMap<String, String> resolveComponentSettings(String name, IndexPolicyTypeEnum expectedType) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        TreeMap<String, String> settings = new TreeMap<>();
        IndexPolicy policy = findPolicy(name, expectedType);
        if (policy != null) {
            if (policy.isInvalid()) {
                return null;
            }
            if (policy.getProperties() != null) {
                settings.putAll(policy.getProperties());
            }
        }
        String type = normalizeBuiltinComponentName(
                settings.isEmpty() ? name : settings.get(IndexPolicy.PROP_TYPE), expectedType);
        if (type == null) {
            return null;
        }
        settings.put(IndexPolicy.PROP_TYPE, type);
        return settings;
    }

    // Escaped entries keep the conservative answer rather than reproducing BE unescaping.
    private static boolean tokenizeOnCharsIgnoreAsciiLetters(String value) {
        if (value == null) {
            return true;
        }
        List<String> entries = parseEntryList(value);
        if (entries == null) {
            return false;
        }
        for (String entry : entries) {
            if (CHAR_GROUP_TYPES.contains(entry)) {
                continue;
            }
            if (entry.indexOf('\\') >= 0 || entry.codePointCount(0, entry.length()) != 1) {
                return false;
            }
            int codePoint = entry.codePointAt(0);
            if ((codePoint >= 'A' && codePoint <= 'Z') || (codePoint >= 'a' && codePoint <= 'z')) {
                return false;
            }
        }
        return true;
    }

    /**
     * The first token filter that folds ASCII case, reached before any filter that could tell an
     * upper-case letter from its lower-case form, or null when there is none.
     */
    private static FoldContext tokenFiltersFoldAsciiCase(String filterList) {
        if (Strings.isNullOrEmpty(filterList)) {
            return null;
        }
        for (String filterName : filterList.split(",\\s*")) {
            TreeMap<String, String> settings = resolveComponentSettings(
                    filterName.trim(), IndexPolicyTypeEnum.TOKEN_FILTER);
            if (settings == null) {
                return null;
            }
            switch (settings.get(IndexPolicy.PROP_TYPE)) {
                case "lowercase":
                    return FoldContext.unfiltered();
                case "empty":
                case "asciifolding":
                    // ASCII bytes pass through ASCII folding unchanged.
                    continue;
                case "icu_normalizer":
                    FoldContext fold = icuNormalizerFoldContext(settings);
                    if (fold != null) {
                        return fold;
                    }
                    if (isAsciiCaseTransparentIcuNormalizer(settings)) {
                        continue;
                    }
                    return null;
                default:
                    return null;
            }
        }
        return null;
    }
}
