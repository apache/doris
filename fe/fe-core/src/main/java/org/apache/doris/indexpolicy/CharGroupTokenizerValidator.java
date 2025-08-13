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

public class CharGroupTokenizerValidator extends BasePolicyValidator {
    private static final Set<String> ALLOWED_PROPS = ImmutableSet.of(
            "type", "max_token_length", "tokenize_on_chars");

    private static final Set<String> VALID_CHAR_TYPES = ImmutableSet.of(
            "letter", "digit", "whitespace", "punctuation", "symbol", "cjk");

    public CharGroupTokenizerValidator() {
        super(ALLOWED_PROPS);
    }

    @Override
    protected String getTypeName() {
        return "char group tokenizer";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
        // max_token_length
        if (props.containsKey("max_token_length")) {
            try {
                int v = Integer.parseInt(props.get("max_token_length"));
                if (v <= 0) {
                    throw new DdlException("max_token_length must be a positive integer (default: 255)");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("max_token_length must be a positive integer (default: 255)");
            }
        }

        // tokenize_on_chars: only check bracketed format and non-empty content
        if (props.containsKey("tokenize_on_chars")) {
            String raw = props.get("tokenize_on_chars");
            if (raw == null || raw.trim().isEmpty()) {
                throw new DdlException("tokenize_on_chars cannot be empty if specified");
            }
            String[] items = raw.split("\\s*,\\s*");
            for (String item : items) {
                String trimmed = item.trim();
                if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
                    throw new DdlException("Each item in tokenize_on_chars must be enclosed in square brackets. "
                            + "Invalid item: " + item);
                }
                String content = trimmed.substring(1, trimmed.length() - 1);
                if (content.length() == 0) {
                    throw new DdlException("tokenize_on_chars cannot contain empty items: " + item);
                }
                validateTokenizeOnCharsContent(content, item);
            }
        }
    }

    private void validateTokenizeOnCharsContent(String content, String originalItem) throws DdlException {
        if (VALID_CHAR_TYPES.contains(content)) {
            return;
        }
        if (content.startsWith("\\")) {
            return;
        }
        if (content.codePointCount(0, content.length()) != 1) {
            throw new DdlException("Invalid tokenize_on_chars item: " + originalItem + ". "
                    + "Content must be either a valid character type (" + VALID_CHAR_TYPES + "), "
                    + "an escaped character (starting with \\), or a single unicode character.");
        }
    }
}
