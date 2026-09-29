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

import org.apache.doris.analysis.invertedindex.AnalyzerIdentityBuilder;
import org.apache.doris.analysis.invertedindex.AnalyzerKeyNormalizer;
import org.apache.doris.analysis.invertedindex.InvertedIndexSqlGenerator;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class InvertedIndexUtil {
    private static final Logger LOG = LogManager.getLogger(InvertedIndexUtil.class);

    // A MATCH analyzer name may select an index by its analyzer or its normalizer.
    private static final Set<String> BUILTIN_TOP_LEVEL_NAMES = ImmutableSet.<String>builder()
            .addAll(IndexPolicy.BUILTIN_ANALYZERS)
            .addAll(IndexPolicy.BUILTIN_NORMALIZERS)
            .build();

    public static String INVERTED_INDEX_PARSER_UNKNOWN = "unknown";
    public static String INVERTED_INDEX_PARSER_KEY = InvertedIndexProperties.INVERTED_INDEX_PARSER_KEY;
    public static String INVERTED_INDEX_PARSER_KEY_ALIAS = InvertedIndexProperties.INVERTED_INDEX_PARSER_KEY_ALIAS;
    public static String INVERTED_INDEX_PARSER_NONE = InvertedIndexProperties.INVERTED_INDEX_PARSER_NONE;
    public static String INVERTED_INDEX_PARSER_STANDARD = InvertedIndexProperties.INVERTED_INDEX_PARSER_STANDARD;
    public static String INVERTED_INDEX_PARSER_UNICODE = InvertedIndexProperties.INVERTED_INDEX_PARSER_UNICODE;
    public static String INVERTED_INDEX_PARSER_ENGLISH = InvertedIndexProperties.INVERTED_INDEX_PARSER_ENGLISH;
    public static String INVERTED_INDEX_PARSER_CHINESE = InvertedIndexProperties.INVERTED_INDEX_PARSER_CHINESE;
    public static String INVERTED_INDEX_PARSER_ICU = InvertedIndexProperties.INVERTED_INDEX_PARSER_ICU;
    public static String INVERTED_INDEX_PARSER_BASIC = InvertedIndexProperties.INVERTED_INDEX_PARSER_BASIC;
    public static String INVERTED_INDEX_PARSER_IK = InvertedIndexProperties.INVERTED_INDEX_PARSER_IK;
    public static String INVERTED_INDEX_PARSER_KUROMOJI = InvertedIndexProperties.INVERTED_INDEX_PARSER_KUROMOJI;

    public static String INVERTED_INDEX_PARSER_MODE_KEY = "parser_mode";
    public static String INVERTED_INDEX_PARSER_FINE_GRANULARITY = "fine_grained";
    public static String INVERTED_INDEX_PARSER_COARSE_GRANULARITY = "coarse_grained";
    public static String INVERTED_INDEX_PARSER_MAX_WORD = "ik_max_word";
    public static String INVERTED_INDEX_PARSER_SMART = "ik_smart";

    public static String INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE = "char_filter_type";
    public static String INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN = "char_filter_pattern";
    public static String INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT = "char_filter_replacement";

    public static String INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE = "char_replace";

    public static String INVERTED_INDEX_SUPPORT_PHRASE_KEY = "support_phrase";

    public static String INVERTED_INDEX_NORMS_KEY = "norms";

    public static String INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY = "ignore_above";

    public static String INVERTED_INDEX_PARSER_LOWERCASE_KEY = "lower_case";

    public static String INVERTED_INDEX_PARSER_STOPWORDS_KEY = "stopwords";

    public static String INVERTED_INDEX_DICT_COMPRESSION_KEY = "dict_compression";

    public static String INVERTED_INDEX_ANALYZER_NAME_KEY = "analyzer";
    public static String INVERTED_INDEX_NORMALIZER_NAME_KEY = "normalizer";

    public static String INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY = "field_pattern";

    // Default analyzer key constant - matches BE's INVERTED_INDEX_DEFAULT_ANALYZER_KEY
    public static final String INVERTED_INDEX_DEFAULT_ANALYZER_KEY = "__default__";

    public static String getInvertedIndexParser(Map<String, String> properties) {
        if (properties == null) {
            return INVERTED_INDEX_PARSER_NONE;
        }
        String parser = properties.get(INVERTED_INDEX_PARSER_KEY);
        if (parser == null) {
            parser = properties.get(INVERTED_INDEX_PARSER_KEY_ALIAS);
        }
        return parser != null ? parser : INVERTED_INDEX_PARSER_NONE;
    }

    public static String getInvertedIndexParserMode(Map<String, String> properties) {
        if (properties == null) {
            return INVERTED_INDEX_PARSER_COARSE_GRANULARITY;
        }
        String mode = properties.get(INVERTED_INDEX_PARSER_MODE_KEY);
        String parser = properties.get(INVERTED_INDEX_PARSER_KEY);
        if (parser == null) {
            parser = properties.get(INVERTED_INDEX_PARSER_KEY_ALIAS);
        }
        return mode != null ? mode :
            INVERTED_INDEX_PARSER_IK.equals(parser) ? INVERTED_INDEX_PARSER_SMART :
                INVERTED_INDEX_PARSER_COARSE_GRANULARITY;
    }

    public static String getInvertedIndexFieldPattern(Map<String, String> properties) {
        String fieldPattern = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY);
        // default is "none" if not set
        return fieldPattern != null ? fieldPattern : "";
    }

    public static boolean getInvertedIndexSupportPhrase(Map<String, String> properties) {
        String supportPhrase = properties == null ? null : properties.get(INVERTED_INDEX_SUPPORT_PHRASE_KEY);
        return supportPhrase != null ? Boolean.parseBoolean(supportPhrase) : true;
    }

    public static String getPreferredAnalyzer(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return "";
        }
        // Check analyzer first, then normalizer
        String analyzer = properties.get(INVERTED_INDEX_ANALYZER_NAME_KEY);
        if (analyzer != null && !analyzer.isEmpty()) {
            return analyzer;
        }
        String normalizer = properties.get(INVERTED_INDEX_NORMALIZER_NAME_KEY);
        return normalizer != null ? normalizer : "";
    }

    public static Map<String, String> getInvertedIndexCharFilter(Map<String, String> properties) {
        if (properties == null) {
            return new HashMap<>();
        }

        if (!properties.containsKey(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE)) {
            return new HashMap<>();
        }
        String type = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE);

        Map<String, String> charFilterMap = new HashMap<>();
        if (type.equals(INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE)) {
            // type
            charFilterMap.put(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE);

            // pattern
            if (!properties.containsKey(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN)) {
                return new HashMap<>();
            }
            String pattern = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN);
            charFilterMap.put(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, pattern);

            // placement
            String replacement = " ";
            if (properties.containsKey(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT)) {
                replacement = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT);
            }
            charFilterMap.put(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, replacement);
        } else {
            return new HashMap<>();
        }

        return charFilterMap;
    }

    public static boolean getInvertedIndexParserLowercase(Map<String, String> properties) {
        String lowercase = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_LOWERCASE_KEY);
        // default is true if not set
        return lowercase != null ? Boolean.parseBoolean(lowercase) : true;
    }

    public static String getInvertedIndexParserStopwords(Map<String, String> properties) {
        String stopwrods = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_STOPWORDS_KEY);
        // default is "" if not set
        return stopwrods != null ? stopwrods : "";
    }

    public static String getInvertedIndexAnalyzerName(Map<String, String> properties) {
        if (properties == null) {
            return "";
        }

        String analyzerName = properties.get(INVERTED_INDEX_ANALYZER_NAME_KEY);
        if (analyzerName != null && !analyzerName.isEmpty()) {
            return analyzerName;
        }

        String normalizerName = properties.get(INVERTED_INDEX_NORMALIZER_NAME_KEY);
        return normalizerName != null ? normalizerName : "";
    }

    /**
     * Scalar column types the SNII storage format can serve with its native BKD index. Mirrors
     * {@code field_is_numeric_type} on the BE side, which is what routes the column to
     * SniiBkdIndexColumnWriter / SniiBkdIndexReader. Kept in sync with
     * {@code IndexDefinition.isSupportSniiNumericIdxType}, which applies the same rule one layer up
     * where the ARRAY item type is also known.
     */
    public static boolean isSupportSniiNumericIdxType(PrimitiveType colType) {
        return colType.isNumericType() || colType.isDateLikeType() || colType.isTimeStampTzType()
                || colType.isIPType() || colType == PrimitiveType.BOOLEAN;
    }

    public static void checkInvertedIndexParser(String indexColName, PrimitiveType colType,
            Map<String, String> properties,
            TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat) throws AnalysisException {
        String parser = null;
        if (properties != null) {
            parser = properties.get(INVERTED_INDEX_PARSER_KEY);
            if (parser == null) {
                parser = properties.get(INVERTED_INDEX_PARSER_KEY_ALIAS);
            }
            checkInvertedIndexProperties(properties, colType, invertedIndexFileStorageFormat);
        }

        // A whole-column VARIANT index reaches here with the parent type, which decides
        // nothing: the sub-column type is checked on the field_pattern path instead.
        if (invertedIndexFileStorageFormat == TInvertedIndexFileStorageFormat.SNII
                && !colType.isStringType() && !colType.isArrayType()
                && colType != PrimitiveType.VARIANT
                && !isSupportSniiNumericIdxType(colType)) {
            throw new AnalysisException("SNII inverted index storage format does not support index on column: "
                    + indexColName + " type: " + colType);
        }

        // default is "none" if not set
        if (parser == null) {
            parser = INVERTED_INDEX_PARSER_NONE;
        }

        // array type is not supported parser except "none"
        if (colType.isArrayType() && !parser.equals(INVERTED_INDEX_PARSER_NONE)) {
            throw new AnalysisException("INVERTED index with parser: " + parser
                + " is not supported for array column: " + indexColName);
        }

        if (colType.isStringType() || colType.isVariantType()) {
            if (!(parser.equals(INVERTED_INDEX_PARSER_NONE)
                    || parser.equals(INVERTED_INDEX_PARSER_STANDARD)
                        || parser.equals(INVERTED_INDEX_PARSER_UNICODE)
                            || parser.equals(INVERTED_INDEX_PARSER_ENGLISH)
                                || parser.equals(INVERTED_INDEX_PARSER_CHINESE)
                                    || parser.equals(INVERTED_INDEX_PARSER_ICU)
                                        || parser.equals(INVERTED_INDEX_PARSER_BASIC)
                                            || parser.equals(INVERTED_INDEX_PARSER_IK))) {
                throw new AnalysisException("INVERTED index parser: " + parser
                    + " is invalid for column: " + indexColName + " of type " + colType);
            }
        } else if (!parser.equals(INVERTED_INDEX_PARSER_NONE)) {
            throw new AnalysisException("INVERTED index with parser: " + parser
                + " is not supported for column: " + indexColName + " of type " + colType);
        }
    }

    private static boolean isSingleByte(String str) {
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) > 0xFF) {
                return false;
            }
        }
        return true;
    }

    public static void checkInvertedIndexProperties(Map<String, String> properties, PrimitiveType colType,
            TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat) throws AnalysisException {
        Set<String> allowedKeys = new HashSet<>(Arrays.asList(
                INVERTED_INDEX_PARSER_KEY,
                INVERTED_INDEX_PARSER_KEY_ALIAS,
                INVERTED_INDEX_PARSER_MODE_KEY,
                INVERTED_INDEX_SUPPORT_PHRASE_KEY,
                INVERTED_INDEX_NORMS_KEY,
                INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE,
                INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN,
                INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT,
                INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY,
                INVERTED_INDEX_PARSER_LOWERCASE_KEY,
                INVERTED_INDEX_PARSER_STOPWORDS_KEY,
                INVERTED_INDEX_DICT_COMPRESSION_KEY,
                INVERTED_INDEX_ANALYZER_NAME_KEY,
                INVERTED_INDEX_NORMALIZER_NAME_KEY,
                INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY
        ));

        for (String key : properties.keySet()) {
            if (!allowedKeys.contains(key)) {
                throw new AnalysisException("Invalid inverted index property key: " + key);
            }
        }

        String parser = properties.get(INVERTED_INDEX_PARSER_KEY);
        if (parser == null) {
            parser = properties.get(INVERTED_INDEX_PARSER_KEY_ALIAS);
        }
        String parserMode = properties.get(INVERTED_INDEX_PARSER_MODE_KEY);
        String supportPhrase = properties.get(INVERTED_INDEX_SUPPORT_PHRASE_KEY);
        String charFilterType = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE);
        String charFilterPattern = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN);
        String charFilterReplacement = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT);
        String ignoreAbove = properties.get(INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY);
        String lowerCase = properties.get(INVERTED_INDEX_PARSER_LOWERCASE_KEY);
        String stopWords = properties.get(INVERTED_INDEX_PARSER_STOPWORDS_KEY);
        String dictCompression = properties.get(INVERTED_INDEX_DICT_COMPRESSION_KEY);
        String analyzerName = properties.get(INVERTED_INDEX_ANALYZER_NAME_KEY);
        String normalizerName = properties.get(INVERTED_INDEX_NORMALIZER_NAME_KEY);

        int configCount = 0;
        if (analyzerName != null && !analyzerName.isEmpty()) {
            configCount++;
        }
        if (parser != null && !parser.isEmpty()) {
            configCount++;
        }
        if (normalizerName != null && !normalizerName.isEmpty()) {
            configCount++;
        }

        if (configCount > 1) {
            throw new AnalysisException(
                    "Cannot specify more than one of 'analyzer', 'parser', or 'normalizer' properties. "
                            + "Please choose only one: "
                            + "'analyzer' for custom analyzer, "
                            + "'parser' for built-in parser, "
                            + "or 'normalizer' for text normalization without tokenization.");
        }

        checkAnalyzerName(analyzerName, colType, invertedIndexFileStorageFormat, supportPhrase);
        applyGramFamilyIndexDefaults(analyzerName, properties);
        checkNormalizerName(normalizerName, colType);

        if (parser != null && !parser.matches("none|english|unicode|chinese|standard|icu|basic|ik")) {
            throw new AnalysisException("Invalid inverted index 'parser' value: " + parser
                    + ", parser must be none, english, unicode, chinese, icu, basic or ik");
        }

        if (parserMode != null) {
            if (INVERTED_INDEX_PARSER_CHINESE.equals(parser)) {
                if (!parserMode.matches("fine_grained|coarse_grained")) {
                    throw new AnalysisException("Invalid inverted index 'parser_mode' value: " + parserMode
                        + ", parser_mode must be fine_grained or coarse_grained for chinese parser");
                }
            } else if (INVERTED_INDEX_PARSER_IK.equals(parser)) {
                if (!parserMode.matches("ik_max_word|ik_smart")) {
                    throw new AnalysisException("Invalid inverted index 'parser_mode' value: " + parserMode
                        + ", parser_mode must be ik_max_word or ik_smart for ik parser");
                }
            } else if (parserMode != null) {
                throw new AnalysisException("parser_mode is only available for chinese and ik parser");
            }
        }

        if (supportPhrase != null && !supportPhrase.matches("true|false")) {
            throw new AnalysisException("Invalid inverted index 'support_phrase' value: " + supportPhrase
                    + ", support_phrase must be true or false");
        }

        String norms = properties.get(INVERTED_INDEX_NORMS_KEY);
        if (norms != null && !norms.matches("true|false")) {
            throw new AnalysisException("Invalid inverted index 'norms' value: " + norms
                    + ", norms must be true or false");
        }

        if (charFilterType != null) {
            if (!INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE.equals(charFilterType)) {
                throw new AnalysisException("Invalid 'char_filter_type', only '"
                    + INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE + "' is supported");
            }
            if (charFilterPattern == null || charFilterPattern.isEmpty()) {
                throw new AnalysisException("Missing 'char_filter_pattern' for 'char_replace' filter type");
            }
            if (!isSingleByte(charFilterPattern)) {
                throw new AnalysisException("'char_filter_pattern' must contain only ASCII characters");
            }
            if (charFilterReplacement != null && !charFilterReplacement.isEmpty()) {
                if (!isSingleByte(charFilterReplacement)) {
                    throw new AnalysisException("'char_filter_replacement' must contain only ASCII characters");
                }
            }
        }

        if (ignoreAbove != null) {
            try {
                int ignoreAboveValue = Integer.parseInt(ignoreAbove);
                if (ignoreAboveValue <= 0) {
                    throw new AnalysisException("Invalid inverted index 'ignore_above' value: " + ignoreAboveValue
                            + ", ignore_above must be positive");
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                        "Invalid inverted index 'ignore_above' value, ignore_above must be integer");
            }
        }

        if (lowerCase != null && !lowerCase.matches("true|false")) {
            throw new AnalysisException(
                    "Invalid inverted index 'lower_case' value: " + lowerCase + ", lower_case must be true or false");
        }

        if (stopWords != null && !stopWords.matches("none")) {
            throw new AnalysisException("Invalid inverted index 'stopWords' value: " + stopWords
                    + ", stopWords must be none");
        }

        if (dictCompression != null) {
            if (!colType.isStringType() && !colType.isVariantType()) {
                throw new AnalysisException("dict_compression can only be set for StringType columns. type: "
                        + colType);
            }

            if (!dictCompression.matches("true|false")) {
                throw new AnalysisException(
                        "Invalid inverted index 'dict_compression' value: "
                                + dictCompression + ", dict_compression must be true or false");
            }

            if (invertedIndexFileStorageFormat != TInvertedIndexFileStorageFormat.V3) {
                throw new AnalysisException(
                        "dict_compression can only be set when storage format is V3");
            }
        }

        // Canonicalize built-ins while retaining the exact spelling of a resolved legacy policy.
        normalizeInvertedIndexProperties(properties);
    }

    /**
     * Canonicalize analyzer and normalizer names in index properties. Legacy metadata may contain
     * case-distinct policy names, so a resolved custom policy must keep its exact stored name.
     */
    public static void normalizeInvertedIndexProperties(Map<String, String> properties) {
        resolvePolicyNames(properties);
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(
                properties,
                INVERTED_INDEX_PARSER_KEY,
                INVERTED_INDEX_PARSER_KEY_ALIAS);
    }

    /** Store analyzer and normalizer names in the spelling BE dispatches on. */
    public static void resolvePolicyNames(Map<String, String> properties) {
        normalizeResolvedPolicyName(properties, INVERTED_INDEX_ANALYZER_NAME_KEY, IndexPolicy.BUILTIN_ANALYZERS);
        normalizeResolvedPolicyName(properties, INVERTED_INDEX_NORMALIZER_NAME_KEY, IndexPolicy.BUILTIN_NORMALIZERS);
    }

    private static void normalizeResolvedPolicyName(Map<String, String> properties, String key,
            Set<String> builtins) {
        String name = properties.get(key);
        if (name == null || name.isEmpty()) {
            return;
        }
        properties.put(key, resolveAnalyzerName(name, builtins));
    }

    /** Resolve built-in names and retain the stored spelling of custom policies. */
    public static String resolveAnalyzerName(String name) {
        return resolveAnalyzerName(name, BUILTIN_TOP_LEVEL_NAMES);
    }

    // Validation resolves in the same order, so the stored name binds what it accepted.
    private static String resolveAnalyzerName(String name, Set<String> builtins) {
        String trimmedName = name.trim();
        IndexPolicyMgr policyMgr = Env.getCurrentEnv().getIndexPolicyMgr();
        String builtin = policyMgr.getTopLevelBuiltin(trimmedName, builtins);
        if (builtin != null) {
            return builtin;
        }
        IndexPolicy policy = policyMgr.getPolicyByName(trimmedName);
        return policy == null ? trimmedName.toLowerCase(Locale.ROOT) : policy.getName();
    }

    private static void checkAnalyzerName(String analyzerName, PrimitiveType colType,
            TInvertedIndexFileStorageFormat storageFormat, String supportPhrase)
            throws AnalysisException {
        if (analyzerName == null || analyzerName.isEmpty()) {
            return;
        }
        if (!colType.isStringType() && !colType.isVariantType()) {
            throw new AnalysisException("INVERTED index with analyzer: " + analyzerName
                    + " is not supported for column of type " + colType);
        }
        try {
            IndexPolicyMgr indexPolicyMgr = Env.getCurrentEnv().getIndexPolicyMgr();
            indexPolicyMgr.validateAnalyzerExists(analyzerName);
            // Gram-family analyzer (an ngram tokenizer carrying mode, see
            // IndexPolicyMgr#resolveGramTokenizerMode): BE builds sparse/dense gram postings for it
            // only on SNII, and those postings carry no positions, so phrase queries are impossible.
            Optional<String> gramMode = indexPolicyMgr.resolveGramTokenizerMode(analyzerName);
            if (gramMode.isPresent()) {
                if (colType.isArrayType()) {
                    throw new AnalysisException("gram tokenizer (mode=" + gramMode.get()
                            + ") analyzer '" + analyzerName + "' does not support ARRAY columns");
                }
                if (!colType.isCharFamily()) {
                    throw new AnalysisException("gram tokenizer (mode=" + gramMode.get()
                            + ") analyzer '" + analyzerName
                            + "' is supported only on scalar CHAR, VARCHAR, or STRING columns");
                }
                if (storageFormat != TInvertedIndexFileStorageFormat.SNII) {
                    throw new AnalysisException("gram tokenizer (mode=" + gramMode.get()
                            + ") requires inverted_index_storage_format = SNII");
                }
                if ("true".equals(supportPhrase)) {
                    throw new AnalysisException(
                            "gram tokenizer index does not support phrase (support_phrase must be false)");
                }
            }
        } catch (DdlException e) {
            throw new AnalysisException("Invalid custom analyzer: " + e.getMessage());
        }
    }

    /**
     * Constraints a gram-family analyzer (an ngram tokenizer carrying mode) enforces at the index
     * property level:
     * 1) an index-level char_filter (char_filter_type/pattern/replacement) conflicts with the
     *    semantics of gram split boundaries (the character replacement happens after the tokenizer
     *    has already split by the gram rule, which breaks reproducibility), so it is rejected;
     * 2) when support_phrase is not given explicitly it defaults to "false", overriding the general
     *    rule of the {@link Index} constructor that "an analyzer implies true" -- a gram index is
     *    forced to docs-only on the BE side and has no positions for a phrase query to use.
     *
     * <p>The {@code properties} held by the caller ({@link #checkInvertedIndexProperties}) and the
     * {@link IndexDefinition} field are the same mutable Map reference, so the defaults written
     * here are seen when {@code IndexDefinition#translateToCatalogStyle} builds the {@link Index}.
     */
    private static void applyGramFamilyIndexDefaults(String analyzerName, Map<String, String> properties)
            throws AnalysisException {
        if (analyzerName == null || analyzerName.isEmpty()) {
            return;
        }
        Optional<String> gramMode = Env.getCurrentEnv().getIndexPolicyMgr().resolveGramTokenizerMode(analyzerName);
        if (!gramMode.isPresent()) {
            return;
        }
        if (properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE) != null
                || properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN) != null
                || properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT) != null) {
            throw new AnalysisException("char_filter cannot be used with gram tokenizer (mode="
                    + gramMode.get() + ")");
        }
        if (properties.get(INVERTED_INDEX_SUPPORT_PHRASE_KEY) == null) {
            properties.put(INVERTED_INDEX_SUPPORT_PHRASE_KEY, "false");
        }
    }

    private static void checkNormalizerName(String normalizerName, PrimitiveType colType) throws AnalysisException {
        if (normalizerName == null || normalizerName.isEmpty()) {
            return;
        }
        if (!colType.isStringType() && !colType.isVariantType()) {
            throw new AnalysisException("INVERTED index with normalizer: " + normalizerName
                    + " is not supported for column of type " + colType);
        }
        try {
            Env.getCurrentEnv().getIndexPolicyMgr().validateNormalizerExists(normalizerName);
        } catch (DdlException e) {
            throw new AnalysisException("Invalid normalizer: " + e.getMessage());
        }
    }

    public static boolean canHaveMultipleInvertedIndexes(DataType colType, List<IndexDefinition> indexDefs) {
        if (indexDefs.size() <= 1) {
            return true;
        }
        if (!colType.isStringLikeType() && !colType.isVariantType()) {
            return false;
        }

        Set<String> analyzerKeys = new HashSet<>();
        Set<String> analyzerSelectors = new HashSet<>();
        for (IndexDefinition indexDef : indexDefs) {
            Map<String, String> properties = indexDef.getProperties();
            String key = buildAnalyzerIdentity(properties);
            // HashSet.add() returns false if element already exists
            if (!analyzerKeys.add(key)) {
                return false;
            }
            String selector = getAnalyzerSelector(properties);
            if (!INVERTED_INDEX_PARSER_IK.equals(selector) && !analyzerSelectors.add(selector)) {
                return false;
            }
        }
        return true;
    }

    private static String getAnalyzerSelector(Map<String, String> properties) {
        String preferredAnalyzer = InvertedIndexProperties.getPreferredAnalyzer(properties);
        if (!Strings.isNullOrEmpty(preferredAnalyzer)) {
            return resolveAnalyzerName(preferredAnalyzer);
        }
        String parser = InvertedIndexProperties.getInvertedIndexParser(properties);
        return Strings.isNullOrEmpty(parser)
                ? InvertedIndexProperties.INVERTED_INDEX_DEFAULT_ANALYZER_KEY
                : parser.trim().toLowerCase(Locale.ROOT);
    }

    public static boolean hasSameNonIkAnalyzerSelector(
            Map<String, String> leftProperties, Map<String, String> rightProperties) {
        String leftSelector = getAnalyzerSelector(leftProperties);
        return !INVERTED_INDEX_PARSER_IK.equals(leftSelector)
                && leftSelector.equals(getAnalyzerSelector(rightProperties));
    }

    public static String buildAnalyzerIdentity(Map<String, String> properties) {
        String preferredAnalyzer = getPreferredAnalyzer(properties);
        String parser = getInvertedIndexParser(properties);
        return AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                properties,
                preferredAnalyzer,
                parser,
                INVERTED_INDEX_DEFAULT_ANALYZER_KEY,
                INVERTED_INDEX_PARSER_NONE,
                LOG);
    }

    public static boolean isAnalyzerMatched(Map<String, String> properties, String analyzer) {
        String normalizedAnalyzer = Strings.isNullOrEmpty(analyzer) ? "" : analyzer.trim();

        if (Strings.isNullOrEmpty(normalizedAnalyzer)) {
            return INVERTED_INDEX_DEFAULT_ANALYZER_KEY.equals(buildAnalyzerIdentity(properties));
        }

        String resolvedAnalyzer = resolveAnalyzerName(normalizedAnalyzer);
        return isAnalyzerNameMatched(properties, normalizedAnalyzer)
                && (!INVERTED_INDEX_PARSER_IK.equals(resolvedAnalyzer)
                    || matchesBuiltinIkDefaults(properties));
    }

    /**
     * Whether the index is served by the named analyzer, regardless of how a built-in IK index is
     * configured. This name check is all that selected an index before built-in IK indexes were
     * matched by their effective configuration.
     */
    public static boolean isAnalyzerNameMatched(Map<String, String> properties, String analyzer) {
        String normalizedAnalyzer = Strings.isNullOrEmpty(analyzer) ? "" : analyzer.trim();
        if (normalizedAnalyzer.isEmpty()) {
            return false;
        }
        String resolvedAnalyzer = resolveAnalyzerName(normalizedAnalyzer);
        String preferredAnalyzer = InvertedIndexProperties.getPreferredAnalyzer(properties);
        if (!Strings.isNullOrEmpty(preferredAnalyzer)) {
            return resolvedAnalyzer.equals(resolveAnalyzerName(preferredAnalyzer));
        }

        String parser = getInvertedIndexParser(properties);
        if (Strings.isNullOrEmpty(parser)) {
            return resolvedAnalyzer.equals("default")
                    || resolvedAnalyzer.equals(INVERTED_INDEX_PARSER_NONE);
        }
        return resolvedAnalyzer.equals(parser.trim().toLowerCase(Locale.ROOT));
    }

    private static boolean matchesBuiltinIkDefaults(Map<String, String> properties) {
        return buildAnalyzerIdentity(properties).equals(
                buildAnalyzerIdentity(ImmutableMap.of(INVERTED_INDEX_ANALYZER_NAME_KEY, INVERTED_INDEX_PARSER_IK)));
    }

    /**
     * Builds the SQL fragment for USING ANALYZER clause.
     * Returns empty string if analyzer is null or empty.
     * Otherwise returns " USING ANALYZER <analyzer>" with proper quoting.
     */
    public static String buildAnalyzerSqlFragment(String analyzer) {
        return InvertedIndexSqlGenerator.buildAnalyzerSqlFragment(analyzer);
    }
}
