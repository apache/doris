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

    // gram 族（稀疏/稠密 gram 索引）支持的 mode 取值，大小写不敏感
    private static final Set<String> VALID_MODES = ImmutableSet.of("auto", "sparse", "dense");

    public NGramTokenizerValidator() {
        super(ALLOWED_PROPS);
    }

    @Override
    protected String getTypeName() {
        return "ngram tokenizer";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
        // gram 族新参数：一旦指定 mode，就说明这是 auto/sparse/dense gram 索引，
        // 校验规则与 legacy ngram 完全独立，校验完直接返回，不再复用下面的 legacy 规则。
        String mode = props.get("mode");
        if (mode != null) {
            validateGramMode(props, mode);
            return;
        }
        // 未指定 mode 时，density/stop_gram_df/lower_case 没有意义，直接拒绝
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
     * 校验 gram 族（auto/sparse/dense）参数：mode 本身的取值范围、min/max_gram 的默认值与顺序关系、
     * density/stop_gram_df/lower_case 的取值范围，以及 token_chars 系列与 mode 的互斥关系。
     * 空字符串 mode（未在白名单校验阶段被拦截）也会在这里因不属于 VALID_MODES 而被拒绝。
     */
    private void validateGramMode(Map<String, String> props, String mode) throws DdlException {
        if (!VALID_MODES.contains(mode.toLowerCase())) {
            throw new DdlException("ngram tokenizer mode must be one of " + VALID_MODES + ", got: " + mode);
        }
        int minGram = parsePositiveInt(props, "min_gram", 3);
        int maxGram = parsePositiveInt(props, "max_gram", 16);
        if (minGram > maxGram) {
            throw new DdlException("min_gram (" + minGram + ") must be <= max_gram (" + maxGram + ")");
        }
        if (props.containsKey("density")) {
            double density = parseDouble(props.get("density"), "density");
            if (!(density > 0.0 && density <= 1.0)) {
                throw new DdlException("density must be in (0, 1], got: " + props.get("density"));
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
     * 解析正整数属性；属性未设置时返回默认值 {@code dflt}。
     */
    private static int parsePositiveInt(Map<String, String> props, String key, int dflt) throws DdlException {
        if (!props.containsKey(key)) {
            return dflt;
        }
        try {
            int value = Integer.parseInt(props.get(key));
            if (value <= 0) {
                throw new DdlException(key + " must be a positive integer, got: " + props.get(key));
            }
            return value;
        } catch (NumberFormatException e) {
            throw new DdlException(key + " must be a positive integer, got: " + props.get(key));
        }
    }

    /**
     * 解析 double 属性，解析失败时抛出携带字段名的 DdlException。
     */
    private static double parseDouble(String value, String key) throws DdlException {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new DdlException(key + " must be a number, got: " + value);
        }
    }
}
