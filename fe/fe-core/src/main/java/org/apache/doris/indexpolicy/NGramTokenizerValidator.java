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

    // gram 族（稀疏/稠密 gram 索引）支持的 mode 取值。
    // 取值必须与 BE 端 `gram_scheme.cpp::GramScheme::from_properties` 完全一致：BE 做的是
    // 精确字符串比较（既不 trim 也不折叠大小写），因此 FE 也只接受严格小写、不带空白的
    // 字面量：" Sparse " / "SPARSE" 一律在 DDL 阶段就拒掉，而不是让 DDL 通过、写入时才在
    // BE 上报 InvalidArgument（FE 不做隐式归一化，否则落盘的策略属性会与用户写的不一致）。
    private static final Set<String> VALID_MODES = ImmutableSet.of("auto", "sparse", "dense");

    // gram 族参数的取值域，逐条对齐 BE `gram_scheme.cpp::from_properties`。
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
     *
     * <p>所有取值域与 BE 端 `gram_scheme.cpp::GramScheme::from_properties` 逐条对齐：
     * min_gram ∈ [1, 64]、max_gram ∈ [1, 256]、density ∈ [0.001, 1]、stop_gram_df ∈ [0, 1]。
     * FE 先拦住越界值，避免 DDL 通过但 BE 解析 gram 方案时才报 InvalidArgument。
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
     * 解析取值范围为 {@code [lo, hi]} 的整数属性；属性未设置时返回默认值 {@code dflt}。
     * 与 BE 端 `gram_scheme.cpp::parse_uint` 同范围，越界与非数字统一报同一条信息。
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
