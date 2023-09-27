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
import org.apache.doris.nereids.types.JsonType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
        } else {
            this.value = jsonNode.toString();
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
}
