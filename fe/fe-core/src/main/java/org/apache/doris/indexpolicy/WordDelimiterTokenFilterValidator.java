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

public class WordDelimiterTokenFilterValidator extends BasePolicyValidator {
    private static final Set<String> ALLOWED_PROPS = ImmutableSet.of(
            "type", "generate_number_parts", "generate_word_parts",
            "protected_words", "split_on_case_change", "split_on_numerics",
            "stem_english_possessive", "type_table"
    );

    private static final Set<String> TYPE_TABLE_VALUES = ImmutableSet.of(
            "ALPHA", "ALPHANUM", "DIGIT", "LOWER", "SUBWORD_DELIM", "UPPER"
    );

    public WordDelimiterTokenFilterValidator() {
        super(ALLOWED_PROPS);
    }

    @Override
    protected String getTypeName() {
        return "word delimiter token filter";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
        // Validate boolean parameters
        validateBooleanParameter(props, "catenate_all", false);
        validateBooleanParameter(props, "catenate_numbers", false);
        validateBooleanParameter(props, "catenate_words", false);
        validateBooleanParameter(props, "generate_number_parts", true);
        validateBooleanParameter(props, "generate_word_parts", true);
        validateBooleanParameter(props, "preserve_original", false);
        validateBooleanParameter(props, "split_on_case_change", true);
        validateBooleanParameter(props, "split_on_numerics", true);
        validateBooleanParameter(props, "stem_english_possessive", true);

        // Validate protected_words (array of strings)
        if (props.containsKey("protected_words")) {
            String protectedWords = props.get("protected_words");
            if (protectedWords == null || protectedWords.trim().isEmpty()) {
                throw new DdlException("protected_words cannot be empty if specified");
            }
        }

        // Validate type_table
        if (props.containsKey("type_table")) {
            String typeTable = props.get("type_table");
            if (typeTable == null || typeTable.trim().isEmpty()) {
                throw new DdlException("type_table cannot be empty if specified");
            }

            // 按逗号分割并去除前后空格
            String[] mappings = typeTable.split("\\s*,\\s*");

            for (String mapping : mappings) {
                mapping = mapping.trim();

                // 强制要求必须用方括号包裹
                if (!mapping.startsWith("[") || !mapping.endsWith("]")) {
                    throw new DdlException("Each mapping must be enclosed in square brackets."
                        + " Invalid mapping: " + mapping);
                }

                // 去除方括号
                String content = mapping.substring(1, mapping.length() - 1).trim();

                // 验证内容格式
                if (!content.contains("=>")) {
                    throw new DdlException("Invalid mapping format inside brackets."
                        + " Expected format: '[chars => TYPE]'");
                }

                String[] parts = content.split("=>");
                if (parts.length != 2) {
                    throw new DdlException("Invalid mapping syntax. Each mapping must contain exactly one '=>'");
                }

                String type = parts[1].trim();
                if (!TYPE_TABLE_VALUES.contains(type)) {
                    throw new DdlException("Invalid type_table type: " + type
                        + ". Valid types are: " + TYPE_TABLE_VALUES);
                }
            }
        }
    }

    private void validateBooleanParameter(Map<String, String> props, String paramName,
            boolean defaultValue) throws DdlException {
        if (props.containsKey(paramName)) {
            String value = props.get(paramName);
            if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                throw new DdlException(paramName + " must be a boolean value (true or false, default: "
                    + defaultValue + ")");
            }
        }
    }
}
