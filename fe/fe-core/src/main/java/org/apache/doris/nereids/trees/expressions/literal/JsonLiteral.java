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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.JsonType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;
import java.util.Map;

/**
 * literal for json type.
 */
public class JsonLiteral extends Literal {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String value;

    /**
     * constructor, will check value.
     */
    public JsonLiteral(String value) {
        super(JsonType.INSTANCE);
        JsonNode jsonNode;
        try {
            jsonNode = MAPPER.readTree(value);
        } catch (JsonProcessingException e) {
            throw new AnalysisException("Invalid jsonb literal: '" + value + "'. because " + e.getMessage());
        }
        if (jsonNode == null || jsonNode.isMissingNode()) {
            throw new AnalysisException("Invalid jsonb literal: ''");
        }
        validateNoLoneSurrogate(jsonNode);
        this.value = jsonNode.toString();
    }

    // RFC 8259 §8.2: JSON strings must not contain lone UTF-16 surrogates.
    // Jackson accepts them by default, so we validate after parsing.
    // Both string values AND object field names are checked.
    private static void validateNoLoneSurrogate(JsonNode node) {
        if (node.isTextual()) {
            validateNoLoneSurrogateInString(node.textValue());
        } else if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                validateNoLoneSurrogateInString(entry.getKey());
                validateNoLoneSurrogate(entry.getValue());
            }
        } else {
            node.forEach(JsonLiteral::validateNoLoneSurrogate);
        }
    }

    private static void validateNoLoneSurrogateInString(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(i + 1))) {
                    throw new AnalysisException(
                            "Invalid jsonb literal: JSON string contains lone high surrogate");
                }
                i++; // skip the paired low surrogate
            } else if (Character.isLowSurrogate(c)) {
                throw new AnalysisException(
                        "Invalid jsonb literal: JSON string contains lone low surrogate");
            }
        }
    }

    @Override
    public String getStringValue() {
        return value;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            return new org.apache.doris.analysis.JsonLiteral(value);
        } catch (Throwable t) {
            throw new AnalysisException(t.getMessage(), t);
        }
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        throw new AnalysisException(String.format("Cast from %s to %s not supported", this, targetType));
    }
}
