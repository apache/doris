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

package org.apache.doris.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Util for json use jackson.
 **/
public class JsonUtil {

    private static final Logger LOG = LogManager.getLogger(JsonUtil.class);

    private static final ObjectMapper objectMapper = new ObjectMapper().configure(
                    DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
            .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
            .configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true).setTimeZone(TimeZone.getDefault())
            .setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    public static JsonNode toJsonNode(Object obj) {
        return objectMapper.valueToTree(obj);
    }

    public static JsonNode readTree(String str) {
        try {
            return objectMapper.readTree(str);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("readTree exception.", e);
        }
    }

    public static ArrayNode parseArray(String text) {
        try {
            return (ArrayNode) objectMapper.readTree(text);
        } catch (Exception e) {
            throw new RuntimeException("parseArray exception.", e);
        }
    }

    /**
     * Parse json text to ObjectNode
     **/
    public static ObjectNode parseObject(String text) {
        try {
            return (ObjectNode) objectMapper.readTree(text);
        } catch (Exception e) {
            throw new RuntimeException("parseObject exception.", e);
        }
    }

    public static <T> T readValue(String text, Class<T> clazz) throws JsonProcessingException {
        return objectMapper.readValue(text, clazz);
    }
}
