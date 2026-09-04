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

package org.apache.doris.indexpolicy;

import org.apache.doris.common.DdlException;

import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NGramTokenizerValidator extends BasePolicyValidator {
    private static final Set<String> ALLOWED_PROPS = ImmutableSet.of(
            "type", "min_gram", "max_gram", "token_chars", "custom_token_chars",
            "mode", "density", "stop_gram_df", "lower_case");

    private static final Set<String> VALID_TOKEN_CHARS = ImmutableSet.of(
            "letter", "digit", "whitespace", "punctuation", "symbol", "custom");

    // The mode values supported by the gram family (sparse/dense gram indexes).
    // They must match BE's `gram_scheme.cpp::GramScheme::from_properties` exactly: BE compares the
    // strings literally (no trimming, no case folding), so FE also accepts only strictly lower-case
    // literals without whitespace: " Sparse " / "SPARSE" are rejected at DDL time instead of letting
    // the DDL pass and having BE report InvalidArgument at write time (FE does no implicit
    // normalization, which would leave the persisted policy properties differing from what the user
    // wrote).
    private static final Set<String> VALID_MODES = ImmutableSet.of("auto", "sparse", "dense");

    // Value domains of the gram-family parameters, mirroring BE's `gram_scheme.cpp::from_properties`.
    private static final int MIN_GRAM_LOWER_BOUND = 1;
    private static final int MIN_GRAM_UPPER_BOUND = 64;
    private static final int MAX_GRAM_LOWER_BOUND = 1;
    private static final int MAX_GRAM_UPPER_BOUND = 256;
    private static final double MIN_DENSITY = 0.001;

    public NGramTokenizerValidator() {
        super(ALLOWED_PROPS);
    }

    @Override
    protected String getTypeName() {
        return "ngram tokenizer";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
        // The new gram-family parameters: once mode is given this is an auto/sparse/dense gram index,
        // whose validation rules are entirely independent of legacy ngram, so validate and return
        // without reusing any of the legacy rules below.
        String mode = props.get("mode");
        if (mode != null) {
            validateGramMode(props, mode);
            return;
        }
        // Without mode, density/stop_gram_df/lower_case are meaningless, so reject them outright
        for (String key : new String[] {"density", "stop_gram_df", "lower_case"}) {
            if (props.containsKey(key)) {
                throw new DdlException("ngram tokenizer parameter '" + key + "' requires mode = auto|sparse|dense");
            }
        }

        int minGram = 1;
        if (props.containsKey("min_gram")) {
            try {
                minGram = Integer.parseInt(props.get("min_gram"));
                if (minGram <= 0) {
                    throw new DdlException("min_gram must be a positive integer (default: 1)");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("min_gram must be a positive integer (default: 1)");
            }
        }

        int maxGram = 2;
        if (props.containsKey("max_gram")) {
            try {
                maxGram = Integer.parseInt(props.get("max_gram"));
                if (maxGram <= 0) {
                    throw new DdlException("max_gram must be a positive integer (default: 2)");
                }
                if (maxGram < minGram) {
                    throw new DdlException("max_gram [" + maxGram + "] "
                        + "cannot be smaller than min_gram [" + minGram + "]");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("max_gram must be a positive integer (default: 2)");
            }
        }

        if (minGram > maxGram) {
            throw new DdlException("max_gram [" + maxGram + "] "
                + "cannot be smaller than min_gram [" + minGram + "]");
        }

        if (props.containsKey("token_chars")) {
            String tokenChars = props.get("token_chars");
            if (!tokenChars.isEmpty()) {
                List<String> charClasses = Arrays.asList(tokenChars.split(","));
                for (String charClass : charClasses) {
                    charClass = charClass.trim();
                    if (!charClass.isEmpty() && !VALID_TOKEN_CHARS.contains(charClass)) {
                        throw new DdlException("Invalid token_chars value [" + charClass + "]. "
                            + "Valid values are: " + VALID_TOKEN_CHARS
                            + " (separated by commas, e.g. 'letter, digit')");
                    }
                }

                if (charClasses.contains("custom") && !props.containsKey("custom_token_chars")) {
                    throw new DdlException("custom_token_chars must be set when token_chars includes 'custom'");
                }
            }
        }

        if (props.containsKey("custom_token_chars")) {
            if (!props.containsKey("token_chars")
                    || !Arrays.asList(props.get("token_chars").split(",")).contains("custom")) {
                throw new DdlException("custom_token_chars can only be used when token_chars includes 'custom'");
            }
        }
    }

    /**
     * Validates the gram-family (auto/sparse/dense) parameters: the value domain of mode itself, the
     * defaults and the ordering of min/max_gram, the value domains of
     * density/stop_gram_df/lower_case, and the mutual exclusion of the token_chars family with mode.
     * An empty mode string (which the allow-list stage does not catch) is rejected here too, for not
     * belonging to VALID_MODES.
     *
     * <p>Every value domain mirrors BE's `gram_scheme.cpp::GramScheme::from_properties` entry by entry:
     * min_gram in [1, 64], max_gram in [1, 256], density in [0.001, 1], stop_gram_df in [0, 1].
     * FE rejects out-of-range values up front, so a DDL cannot pass only for BE to report
     * InvalidArgument when it parses the gram scheme.
     */
    private void validateGramMode(Map<String, String> props, String mode) throws DdlException {
        if (!VALID_MODES.contains(mode)) {
            throw new DdlException("ngram tokenizer mode must be one of " + VALID_MODES
                    + ", got: '" + mode + "'" + (mode.isEmpty() ? " (empty)" : ""));
        }
        int minGram = parseIntInRange(props, "min_gram", 3, MIN_GRAM_LOWER_BOUND, MIN_GRAM_UPPER_BOUND);
        int maxGram = parseIntInRange(props, "max_gram", 16, MAX_GRAM_LOWER_BOUND, MAX_GRAM_UPPER_BOUND);
        if (minGram > maxGram) {
            throw new DdlException("min_gram (" + minGram + ") must be <= max_gram (" + maxGram + ")");
        }
        if (props.containsKey("density")) {
            double density = parseDouble(props.get("density"), "density");
            if (!(density >= MIN_DENSITY && density <= 1.0)) {
                throw new DdlException("density must be in [0.001, 1], got: " + props.get("density"));
            }
        }
        if (props.containsKey("stop_gram_df")) {
            double stopGramDf = parseDouble(props.get("stop_gram_df"), "stop_gram_df");
            if (!(stopGramDf >= 0.0 && stopGramDf <= 1.0)) {
                throw new DdlException("stop_gram_df must be in [0, 1], got: " + props.get("stop_gram_df"));
            }
        }
        if (props.containsKey("lower_case") && !props.get("lower_case").matches("true|false")) {
            throw new DdlException("lower_case must be true or false, got: " + props.get("lower_case"));
        }
        if (props.containsKey("token_chars") || props.containsKey("custom_token_chars")) {
            throw new DdlException("token_chars cannot be used together with mode (gram tokenizer splits by script)");
        }
    }

    /**
     * Parses an integer property whose value range is {@code [lo, hi]}; returns the default
     * {@code dflt} when the property is not set.
     * Same range as BE's `gram_scheme.cpp::parse_uint`; out-of-range and non-numeric values report
     * the same message.
     */
    private static int parseIntInRange(Map<String, String> props, String key, int dflt, int lo, int hi)
            throws DdlException {
        if (!props.containsKey(key)) {
            return dflt;
        }
        String raw = props.get(key);
        try {
            int value = Integer.parseInt(raw);
            if (value < lo || value > hi) {
                throw new DdlException(key + " must be an integer in [" + lo + ", " + hi + "], got: " + raw);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new DdlException(key + " must be an integer in [" + lo + ", " + hi + "], got: " + raw);
        }
    }

    /**
     * Parses a double property, throwing a DdlException that carries the field name on failure.
     */
    private static double parseDouble(String value, String key) throws DdlException {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new DdlException(key + " must be a number, got: " + value);
        }
    }
}
