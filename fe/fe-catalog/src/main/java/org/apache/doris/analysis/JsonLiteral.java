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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;

import java.util.Map;
import java.util.Objects;

public class JsonLiteral extends LiteralExpr {
    private JsonParser parser = new JsonParser();
    @SerializedName("v")
    private String value;
    // Means the converted session variable need to be cast to int, such as "cast 'STRICT_TRANS_TABLES' to Integer".
    private String beConverted = "";

    public JsonLiteral() {
        super();
        type = Type.JSONB;
        this.nullable = false;
    }

    public JsonLiteral(String value) throws AnalysisException {
        try {
            JsonElement element = parser.parse(value);
            validateNoLoneSurrogate(element);
        } catch (JsonSyntaxException e) {
            throw new AnalysisException("Invalid jsonb literal: " + e.getMessage());
        }
        this.value = value;
        type = Type.JSONB;
        this.nullable = false;
    }

    // RFC 8259 §8.2: JSON strings must not contain lone UTF-16 surrogates.
    // Gson accepts them by default, so we validate after parsing.
    private static void validateNoLoneSurrogate(JsonElement element) throws AnalysisException {
        if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
            String s = element.getAsString();
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
        } else if (element.isJsonObject()) {
            for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
                validateNoLoneSurrogate(entry.getValue());
            }
        } else if (element.isJsonArray()) {
            for (JsonElement child : element.getAsJsonArray()) {
                validateNoLoneSurrogate(child);
            }
        }
    }

    protected JsonLiteral(JsonLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new JsonLiteral(this);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        throw new RuntimeException("Not support comparison between JSONB literals");
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitJsonLiteral(this, context);
    }

    @Override
    public String getStringValue() {
        return value;
    }

    public String getUnescapedValue() {
        // Unescape string exactly like Hive does. Hive's method assumes
        // quotes so we add them here to reuse Hive's code.
        return value;
    }

    public String getJsonValue() {
        return value;
    }

    @Override
    public long getLongValue() {
        throw new RuntimeException("JSONB value cannot be parsed as Long value");
    }

    @Override
    public double getDoubleValue() {
        throw new RuntimeException("JSONB value cannot be parsed as Double value");
    }

    @Override
    public String getRealValue() {
        return getJsonValue();
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(value);
    }
}
