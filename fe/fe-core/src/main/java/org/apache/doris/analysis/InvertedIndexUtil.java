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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InvertedIndexUtil {

    public static String INVERTED_INDEX_PARSER_KEY = "parser";
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

    public static String INVERTED_INDEX_CUSTOM_ANALYZER_KEY = "analyzer";

    public static String INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY = "field_pattern";

    public static String getInvertedIndexParser(Map<String, String> properties) {
        String parser = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_KEY);
        // default is "none" if not set
        return parser != null ? parser : INVERTED_INDEX_PARSER_NONE;
    }

    public static String getInvertedIndexParserMode(Map<String, String> properties) {
        String mode = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_MODE_KEY);
        // default is "none" if not set
        String parser = properties.get(INVERTED_INDEX_PARSER_KEY);
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

    public static String getCustomAnalyzer(Map<String, String> properties) {
        String customAnalyzer = properties == null ? null : properties.get(INVERTED_INDEX_CUSTOM_ANALYZER_KEY);
        return customAnalyzer != null ? customAnalyzer : "";
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

    public static String getInvertedIndexCustomAnalyzer(Map<String, String> properties) {
        String customAnalyzer = properties == null ? null : properties.get(INVERTED_INDEX_CUSTOM_ANALYZER_KEY);
        return customAnalyzer != null ? customAnalyzer : "";
    }

    public static void checkInvertedIndexParser(String indexColName, PrimitiveType colType,
            Map<String, String> properties,
            TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat) throws AnalysisException {
        String parser = null;
        if (properties != null) {
            parser = properties.get(INVERTED_INDEX_PARSER_KEY);
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
                INVERTED_INDEX_PARSER_MODE_KEY,
                INVERTED_INDEX_SUPPORT_PHRASE_KEY,
                INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE,
                INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN,
                INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT,
                INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY,
                INVERTED_INDEX_PARSER_LOWERCASE_KEY,
                INVERTED_INDEX_PARSER_STOPWORDS_KEY,
                INVERTED_INDEX_DICT_COMPRESSION_KEY,
                INVERTED_INDEX_CUSTOM_ANALYZER_KEY,
                INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY
        ));

        for (String key : properties.keySet()) {
            if (!allowedKeys.contains(key)) {
                throw new AnalysisException("Invalid inverted index property key: " + key);
            }
        }

        String parser = properties.get(INVERTED_INDEX_PARSER_KEY);
        String parserMode = properties.get(INVERTED_INDEX_PARSER_MODE_KEY);
        String supportPhrase = properties.get(INVERTED_INDEX_SUPPORT_PHRASE_KEY);
        String charFilterType = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE);
        String charFilterPattern = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN);
        String charFilterReplacement = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT);
        String ignoreAbove = properties.get(INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY);
        String lowerCase = properties.get(INVERTED_INDEX_PARSER_LOWERCASE_KEY);
        String stopWords = properties.get(INVERTED_INDEX_PARSER_STOPWORDS_KEY);
        String dictCompression = properties.get(INVERTED_INDEX_DICT_COMPRESSION_KEY);
        String customAnalyzer = properties.get(INVERTED_INDEX_CUSTOM_ANALYZER_KEY);

        if (customAnalyzer != null && !customAnalyzer.isEmpty() && parser != null && !parser.isEmpty()) {
            throw new AnalysisException("Cannot specify both 'parser' and 'custom_analyzer' properties");
        }

        if (customAnalyzer != null && !customAnalyzer.isEmpty()) {
            try {
                Env.getCurrentEnv().getIndexPolicyMgr().validateAnalyzerExists(customAnalyzer);
            } catch (DdlException e) {
                throw new AnalysisException("Invalid custom analyzer: " + e.getMessage());
            }
        }

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
            if (!colType.isStringType()) {
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
    }

    public static boolean canHaveMultipleInvertedIndexes(DataType colType, List<IndexDefinition> indexDefs) {
        if (indexDefs.size() == 0 || indexDefs.size() == 1) {
            return true;
        }
        if (!colType.isStringLikeType() && !colType.isVariantType()) {
            return false;
        }
        if (indexDefs.size() > 2) {
            return false;
        }
        boolean findParsedInvertedIndex = false;
        boolean findNonParsedInvertedIndex = false;
        for (IndexDefinition indexDef : indexDefs) {
            if (indexDef.isAnalyzedInvertedIndex()) {
                findParsedInvertedIndex = true;
            } else {
                findNonParsedInvertedIndex = true;
            }
        }
        if (findParsedInvertedIndex && findNonParsedInvertedIndex) {
            return true;
        }
        return false;
    }
}
