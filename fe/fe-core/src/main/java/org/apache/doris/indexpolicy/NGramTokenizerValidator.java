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
import java.util.regex.Pattern;

public class NGramTokenizerValidator extends BasePolicyValidator {
    private static final Set<String> ALLOWED_PROPS = ImmutableSet.of(
            "type", "min_gram", "max_gram", "token_chars", "custom_token_chars",
            "mode", "density", "lower_case");

    private static final Set<String> VALID_TOKEN_CHARS = ImmutableSet.of(
            "letter", "digit", "whitespace", "punctuation", "symbol", "custom");

    private static final Set<String> VALID_MODES = ImmutableSet.of("auto", "sparse", "dense");
    private static final int MIN_GRAM_LOWER_BOUND = 1;
    private static final int MIN_GRAM_UPPER_BOUND = 64;
    private static final int MAX_GRAM_LOWER_BOUND = 1;
    private static final int MAX_GRAM_UPPER_BOUND = 256;
    private static final int GRAM_MIN_GRAM_DEFAULT = 3;
    private static final int GRAM_MAX_GRAM_DEFAULT = 4;
    private static final double MIN_DENSITY = 0.001;
    private static final Pattern DECIMAL_PATTERN = Pattern.compile(
            "[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?");
    private static final Pattern INTEGER_PATTERN = Pattern.compile("[+-]?[0-9]+");

    public NGramTokenizerValidator() {
        super(ALLOWED_PROPS);
    }

    @Override
    protected String getTypeName() {
        return "ngram tokenizer";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
        String mode = props.get("mode");
        if (mode != null) {
            validateGramMode(props, mode);
            return;
        }
        for (String key : new String[] {"density", "lower_case"}) {
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

    private void validateGramMode(Map<String, String> props, String mode) throws DdlException {
        if (!VALID_MODES.contains(mode)) {
            throw new DdlException("ngram tokenizer mode must be one of " + VALID_MODES
                    + ", got: '" + mode + "'" + (mode.isEmpty() ? " (empty)" : ""));
        }
        int minGram = parseIntInRange(props, "min_gram", GRAM_MIN_GRAM_DEFAULT,
                MIN_GRAM_LOWER_BOUND, MIN_GRAM_UPPER_BOUND);
        int maxGram = parseIntInRange(props, "max_gram", GRAM_MAX_GRAM_DEFAULT,
                MAX_GRAM_LOWER_BOUND, MAX_GRAM_UPPER_BOUND);
        if (minGram > maxGram) {
            throw new DdlException("min_gram (" + minGram + ") must be <= max_gram (" + maxGram + ")");
        }
        if (props.containsKey("density")) {
            double density = parseDouble(props.get("density"), "density");
            if (!(density >= MIN_DENSITY && density <= 1.0)) {
                throw new DdlException("density must be in [0.001, 1], got: " + props.get("density"));
            }
        }
        if (props.containsKey("lower_case") && !props.get("lower_case").matches("true|false")) {
            throw new DdlException("lower_case must be true or false, got: " + props.get("lower_case"));
        }
        if (props.containsKey("token_chars") || props.containsKey("custom_token_chars")) {
            throw new DdlException("token_chars cannot be used together with mode (gram tokenizer splits by script)");
        }
    }

    private static int parseIntInRange(Map<String, String> props, String key, int dflt, int lo, int hi)
            throws DdlException {
        if (!props.containsKey(key)) {
            return dflt;
        }
        String raw = props.get(key);
        if (!INTEGER_PATTERN.matcher(raw).matches()) {
            throw new DdlException(key + " must be an integer in [" + lo + ", " + hi + "], got: " + raw);
        }
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

    private static double parseDouble(String value, String key) throws DdlException {
        if (!DECIMAL_PATTERN.matcher(value).matches()) {
            throw new DdlException(key + " must be a decimal number, got: " + value);
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new DdlException(key + " must be a number, got: " + value);
        }
    }
}
