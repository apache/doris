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
    // A configured range can emit one token per gram size at every input position.
    static final int MAX_NGRAM_DIFF = 255;
    // NGramTokenizer keeps four code-point slots per configured gram plus a refill margin.
    static final int MAX_NGRAM_SIZE = 1024;

    private static final Set<String> ALLOWED_PROPS = ImmutableSet.of(
            "type", "min_gram", "max_gram", "max_ngram_diff", "token_chars", "custom_token_chars");

    private static final Set<String> VALID_TOKEN_CHARS = ImmutableSet.of(
            "letter", "digit", "whitespace", "punctuation", "symbol", "custom");

    public NGramTokenizerValidator() {
        super(ALLOWED_PROPS);
    }

    static boolean isValidPolicy(Map<String, String> properties) {
        try {
            new NGramTokenizerValidator().validate(properties);
            return true;
        } catch (DdlException | RuntimeException e) {
            return false;
        }
    }

    @Override
    protected String getTypeName() {
        return "ngram tokenizer";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
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
        if (minGram > MAX_NGRAM_SIZE || maxGram > MAX_NGRAM_SIZE) {
            throw new DdlException("min_gram and max_gram must be less than or equal to " + MAX_NGRAM_SIZE);
        }

        int maxNgramDiff = 1;
        if (props.containsKey("max_ngram_diff")) {
            String value = props.get("max_ngram_diff");
            if (!value.matches("-?[0-9]+")) {
                throw new DdlException("max_ngram_diff must be a non-negative integer");
            }
            try {
                maxNgramDiff = Integer.parseInt(value);
                if (maxNgramDiff < 0) {
                    throw new DdlException("max_ngram_diff must be greater than or equal to 0");
                }
                if (maxNgramDiff > MAX_NGRAM_DIFF) {
                    throw new DdlException("max_ngram_diff must be less than or equal to " + MAX_NGRAM_DIFF);
                }
            } catch (NumberFormatException e) {
                throw new DdlException("max_ngram_diff must be a non-negative integer");
            }
        }

        int ngramDiff = maxGram - minGram;
        if (ngramDiff > maxNgramDiff) {
            throw new DdlException("The difference between max_gram and min_gram in NGram Tokenizer must be less "
                    + "than or equal to: [ " + maxNgramDiff + " ] but was [" + ngramDiff + "]");
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
}
