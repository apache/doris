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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InvertedIndexUtil {

    public static String INVERTED_INDEX_PARSER_KEY = InvertedIndexProperties.INVERTED_INDEX_PARSER_KEY;
    public static String INVERTED_INDEX_PARSER_KEY_ALIAS = InvertedIndexProperties.INVERTED_INDEX_PARSER_KEY_ALIAS;
    public static String INVERTED_INDEX_PARSER_UNKNOWN = InvertedIndexProperties.INVERTED_INDEX_PARSER_UNKNOWN;
    public static String INVERTED_INDEX_PARSER_NONE = InvertedIndexProperties.INVERTED_INDEX_PARSER_NONE;
    public static String INVERTED_INDEX_PARSER_STANDARD = InvertedIndexProperties.INVERTED_INDEX_PARSER_STANDARD;
    public static String INVERTED_INDEX_PARSER_UNICODE = InvertedIndexProperties.INVERTED_INDEX_PARSER_UNICODE;
    public static String INVERTED_INDEX_PARSER_ENGLISH = InvertedIndexProperties.INVERTED_INDEX_PARSER_ENGLISH;
    public static String INVERTED_INDEX_PARSER_CHINESE = InvertedIndexProperties.INVERTED_INDEX_PARSER_CHINESE;
    public static String INVERTED_INDEX_PARSER_ICU = InvertedIndexProperties.INVERTED_INDEX_PARSER_ICU;
    public static String INVERTED_INDEX_PARSER_BASIC = InvertedIndexProperties.INVERTED_INDEX_PARSER_BASIC;
    public static String INVERTED_INDEX_PARSER_IK = InvertedIndexProperties.INVERTED_INDEX_PARSER_IK;

    public static String INVERTED_INDEX_PARSER_MODE_KEY = InvertedIndexProperties.INVERTED_INDEX_PARSER_MODE_KEY;
    public static String INVERTED_INDEX_PARSER_FINE_GRANULARITY =
            InvertedIndexProperties.INVERTED_INDEX_PARSER_FINE_GRANULARITY;
    public static String INVERTED_INDEX_PARSER_COARSE_GRANULARITY =
            InvertedIndexProperties.INVERTED_INDEX_PARSER_COARSE_GRANULARITY;
    public static String INVERTED_INDEX_PARSER_MAX_WORD = InvertedIndexProperties.INVERTED_INDEX_PARSER_MAX_WORD;
    public static String INVERTED_INDEX_PARSER_SMART = InvertedIndexProperties.INVERTED_INDEX_PARSER_SMART;

    public static String INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE =
            InvertedIndexProperties.INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE;
    public static String INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN =
            InvertedIndexProperties.INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN;
    public static String INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT =
            InvertedIndexProperties.INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT;

    public static String INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE =
            InvertedIndexProperties.INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE;

    public static String INVERTED_INDEX_SUPPORT_PHRASE_KEY =
            InvertedIndexProperties.INVERTED_INDEX_SUPPORT_PHRASE_KEY;

    public static String INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY =
            InvertedIndexProperties.INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY;

    public static String INVERTED_INDEX_PARSER_LOWERCASE_KEY =
            InvertedIndexProperties.INVERTED_INDEX_PARSER_LOWERCASE_KEY;

    public static String INVERTED_INDEX_PARSER_STOPWORDS_KEY =
            InvertedIndexProperties.INVERTED_INDEX_PARSER_STOPWORDS_KEY;

    public static String INVERTED_INDEX_DICT_COMPRESSION_KEY =
            InvertedIndexProperties.INVERTED_INDEX_DICT_COMPRESSION_KEY;

    public static String INVERTED_INDEX_ANALYZER_NAME_KEY =
            InvertedIndexProperties.INVERTED_INDEX_ANALYZER_NAME_KEY;
    public static String INVERTED_INDEX_NORMALIZER_NAME_KEY =
            InvertedIndexProperties.INVERTED_INDEX_NORMALIZER_NAME_KEY;

    public static String INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY =
            InvertedIndexProperties.INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY;

    // Default analyzer key constant - matches BE's INVERTED_INDEX_DEFAULT_ANALYZER_KEY
    public static final String INVERTED_INDEX_DEFAULT_ANALYZER_KEY =
            InvertedIndexProperties.INVERTED_INDEX_DEFAULT_ANALYZER_KEY;

    public static String getInvertedIndexParser(Map<String, String> properties) {
        return InvertedIndexProperties.getInvertedIndexParser(properties);
    }

    public static String getInvertedIndexParserMode(Map<String, String> properties) {
        return InvertedIndexProperties.getInvertedIndexParserMode(properties);
    }

    public static String getInvertedIndexFieldPattern(Map<String, String> properties) {
        return InvertedIndexProperties.getInvertedIndexFieldPattern(properties);
    }

    public static boolean getInvertedIndexSupportPhrase(Map<String, String> properties) {
        return InvertedIndexProperties.getInvertedIndexSupportPhrase(properties);
    }

    public static String getPreferredAnalyzer(Map<String, String> properties) {
        return InvertedIndexProperties.getPreferredAnalyzer(properties);
    }

    public static Map<String, String> getInvertedIndexCharFilter(Map<String, String> properties) {
        return InvertedIndexProperties.getInvertedIndexCharFilter(properties);
    }

    public static boolean getInvertedIndexParserLowercase(Map<String, String> properties) {
        return InvertedIndexProperties.getInvertedIndexParserLowercase(properties);
    }

    public static String getInvertedIndexParserStopwords(Map<String, String> properties) {
        return InvertedIndexProperties.getInvertedIndexParserStopwords(properties);
    }

    public static String getInvertedIndexAnalyzerName(Map<String, String> properties) {
        return InvertedIndexProperties.getInvertedIndexAnalyzerName(properties);
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
        InvertedIndexProperties.normalizeInvertedIndexProperties(properties);
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
        return InvertedIndexProperties.buildAnalyzerIdentity(properties);
    }

    public static boolean isAnalyzerMatched(Map<String, String> properties, String analyzer) {
        return InvertedIndexProperties.isAnalyzerMatched(properties, analyzer);
    }

    /**
     * Builds the SQL fragment for USING ANALYZER clause.
     * Returns empty string if analyzer is null or empty.
     * Otherwise returns " USING ANALYZER <analyzer>" with proper quoting.
     */
    public static String buildAnalyzerSqlFragment(String analyzer) {
        return InvertedIndexProperties.buildAnalyzerSqlFragment(analyzer);
    }
}
