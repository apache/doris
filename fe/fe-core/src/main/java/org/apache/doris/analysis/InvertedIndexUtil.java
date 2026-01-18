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
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InvertedIndexUtil {
    private static final Logger LOG = LogManager.getLogger(InvertedIndexUtil.class);

    public static String INVERTED_INDEX_PARSER_KEY = "parser";
    public static String INVERTED_INDEX_PARSER_KEY_ALIAS = "built_in_analyzer";
    public static String INVERTED_INDEX_PARSER_UNKNOWN = "unknown";
    public static String INVERTED_INDEX_PARSER_NONE = "none";
    public static String INVERTED_INDEX_PARSER_STANDARD = "standard";
    public static String INVERTED_INDEX_PARSER_UNICODE = "unicode";
    public static String INVERTED_INDEX_PARSER_ENGLISH = "english";
    public static String INVERTED_INDEX_PARSER_CHINESE = "chinese";
    public static String INVERTED_INDEX_PARSER_ICU = "icu";
    public static String INVERTED_INDEX_PARSER_BASIC = "basic";
    public static String INVERTED_INDEX_PARSER_IK = "ik";

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

        checkAnalyzerName(analyzerName, colType);
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

        // Normalize analyzer and normalizer names to lowercase for case-insensitive matching
        normalizeInvertedIndexProperties(properties);
    }

    /**
     * Normalize analyzer and normalizer names in index properties to lowercase.
     * This ensures case-insensitive matching between table creation and query time.
     */
    public static void normalizeInvertedIndexProperties(Map<String, String> properties) {
        AnalyzerKeyNormalizer.normalizeInvertedIndexProperties(
                properties,
                INVERTED_INDEX_ANALYZER_NAME_KEY,
                INVERTED_INDEX_NORMALIZER_NAME_KEY,
                INVERTED_INDEX_PARSER_KEY,
                INVERTED_INDEX_PARSER_KEY_ALIAS);
    }

    private static void checkAnalyzerName(String analyzerName, PrimitiveType colType) throws AnalysisException {
        if (analyzerName == null || analyzerName.isEmpty()) {
            return;
        }
        if (!colType.isStringType() && !colType.isVariantType()) {
            throw new AnalysisException("INVERTED index with analyzer: " + analyzerName
                    + " is not supported for column of type " + colType);
        }
        try {
            Env.getCurrentEnv().getIndexPolicyMgr().validateAnalyzerExists(analyzerName);
        } catch (DdlException e) {
            throw new AnalysisException("Invalid custom analyzer: " + e.getMessage());
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
        for (IndexDefinition indexDef : indexDefs) {
            String key = buildAnalyzerIdentity(indexDef.getProperties());
            // HashSet.add() returns false if element already exists
            if (!analyzerKeys.add(key)) {
                return false;
            }
        }
        return true;
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

        String preferredAnalyzer = getPreferredAnalyzer(properties);
        if (!Strings.isNullOrEmpty(preferredAnalyzer)) {
            return normalizedAnalyzer.equalsIgnoreCase(preferredAnalyzer);
        }

        String parser = getInvertedIndexParser(properties);
        if (Strings.isNullOrEmpty(parser)) {
            return normalizedAnalyzer.equalsIgnoreCase("default")
                    || normalizedAnalyzer.equalsIgnoreCase(INVERTED_INDEX_PARSER_NONE);
        }
        return normalizedAnalyzer.equalsIgnoreCase(parser);
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
