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

import java.util.Map;
import java.util.Set;

public class PinyinTokenizerValidator extends BasePolicyValidator {
    private static final Set<String> ALLOWED_PROPS = ImmutableSet.of(
            "type",
            "lowercase",
            "trim_whitespace",
            "keep_none_chinese",
            "keep_none_chinese_in_first_letter",
            "keep_none_chinese_in_joined_full_pinyin",
            "keep_original",
            "keep_first_letter",
            "keep_separate_first_letter",
            "keep_none_chinese_together",
            "none_chinese_pinyin_tokenize",
            "limit_first_letter_length",
            "keep_full_pinyin",
            "keep_joined_full_pinyin",
            "remove_duplicated_term",
            "fixed_pinyin_offset",
            "ignore_pinyin_offset",
            "keep_separate_chinese"
    );

    public PinyinTokenizerValidator() {
        super(ALLOWED_PROPS);
    }

    @Override
    protected String getTypeName() {
        return "pinyin tokenizer";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
        // validate boolean parameter
        validateBooleanParameter(props, "lowercase");
        validateBooleanParameter(props, "trim_whitespace");
        validateBooleanParameter(props, "keep_none_chinese");
        validateBooleanParameter(props, "keep_none_chinese_in_first_letter");
        validateBooleanParameter(props, "keep_none_chinese_in_joined_full_pinyin");
        validateBooleanParameter(props, "keep_original");
        validateBooleanParameter(props, "keep_first_letter");
        validateBooleanParameter(props, "keep_separate_first_letter");
        validateBooleanParameter(props, "keep_none_chinese_together");
        validateBooleanParameter(props, "none_chinese_pinyin_tokenize");
        validateBooleanParameter(props, "keep_full_pinyin");
        validateBooleanParameter(props, "keep_joined_full_pinyin");
        validateBooleanParameter(props, "remove_duplicated_term");
        validateBooleanParameter(props, "fixed_pinyin_offset");
        validateBooleanParameter(props, "ignore_pinyin_offset");
        validateBooleanParameter(props, "keep_separate_chinese");

        if (props.containsKey("limit_first_letter_length")) {
            try {
                int limitLength = Integer.parseInt(props.get("limit_first_letter_length"));
                if (limitLength < 0) {
                    throw new DdlException("limit_first_letter_length must be a non-negative integer (default: 16)");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("limit_first_letter_length must be a non-negative integer (default: 16)");
            }
        }

        validateConfigurationLogic(props);
    }

    /**
     * validate boolean parameter
     */
    private void validateBooleanParameter(Map<String, String> props, String paramName) throws DdlException {
        if (props.containsKey(paramName)) {
            String value = props.get(paramName).toLowerCase();
            if (!"true".equals(value) && !"false".equals(value)) {
                throw new DdlException(paramName + " must be 'true' or 'false'");
            }
        }
    }

    /**
     * validate configuration logic
     */
    private void validateConfigurationLogic(Map<String, String> props) throws DdlException {
        // ensure at least one output format is enabled
        boolean keepFirstLetter = getBooleanValue(props, "keep_first_letter", true);
        boolean keepFullPinyin = getBooleanValue(props, "keep_full_pinyin", true);
        boolean keepJoinedFullPinyin = getBooleanValue(props, "keep_joined_full_pinyin", false);
        boolean keepSeparateFirstLetter = getBooleanValue(props, "keep_separate_first_letter", false);
        boolean keepSeparateChinese = getBooleanValue(props, "keep_separate_chinese", false);

        if (!keepFirstLetter && !keepFullPinyin && !keepJoinedFullPinyin
                && !keepSeparateFirstLetter && !keepSeparateChinese) {
            throw new DdlException("pinyin config error, at least one output format must be enabled "
                    + "(keep_first_letter, keep_separate_first_letter, keep_full_pinyin, "
                    + "keep_joined_full_pinyin, or keep_separate_chinese).");
        }
    }

    /**
     * get boolean value, support default value
     */
    private boolean getBooleanValue(Map<String, String> props, String key, boolean defaultValue) {
        if (!props.containsKey(key)) {
            return defaultValue;
        }
        return "true".equals(props.get(key).toLowerCase());
    }
}
