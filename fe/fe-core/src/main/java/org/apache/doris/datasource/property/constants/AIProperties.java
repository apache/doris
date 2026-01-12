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

package org.apache.doris.datasource.property.constants;

import org.apache.doris.common.DdlException;

import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AIProperties extends BaseProperties {
    public static final String AI_PREFIX = "ai.";

    // required
    public static final String ENDPOINT = "ai.endpoint";
    public static final String PROVIDER_TYPE = "ai.provider_type";
    public static final String MODEL_NAME = "ai.model_name";

    // optional
    public static final String API_KEY = "ai.api_key";
    public static final String TEMPERATURE = "ai.temperature";
    public static final String MAX_TOKEN = "ai.max_token";
    public static final String MAX_RETRIES = "ai.max_retries";
    public static final String RETRY_DELAY_SECOND = "ai.retry_delay_second";
    public static final String ANTHROPIC_VERSION = "ai.anthropic_version";
    public static final String DIMENSIONS = "ai.dimensions";

    // default_val
    public static final String DEFAULT_TEMPERATURE = "-1";
    public static final String DEFAULT_MAX_TOKEN = "-1";
    public static final String DEFAULT_MAX_RETRIES = "3";
    public static final String DEFAULT_RETRY_DELAY_SECOND = "0";
    public static final String DEFAULT_ANTHROPIC_VERSION = "2023-06-01";
    public static final String DEFAULT_DIMENSIONS = "-1";

    public static final String VALIDITY_CHECK = "ai.validity_check";

    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, PROVIDER_TYPE, MODEL_NAME);
    public static final List<String> PROVIDERS
            = Arrays.asList("OPENAI", "LOCAL", "GEMINI", "DEEPSEEK", "ANTHROPIC",
            "MOONSHOT", "QWEN", "MINIMAX", "ZHIPU", "BAICHUAN", "VOYAGEAI");

    public static void requiredAIProperties(Map<String, String> properties) throws DdlException {
        // Check required field
        for (String field : REQUIRED_FIELDS) {
            if (Strings.isNullOrEmpty(properties.get(field))) {
                throw new DdlException("Missing [" + field + "] in properties.");
            }
        }

        // Check the provider is valid
        properties.put(PROVIDER_TYPE, properties.get(PROVIDER_TYPE).toUpperCase());
        if (PROVIDERS.stream().noneMatch(s -> s.equals(properties.get(PROVIDER_TYPE).toUpperCase()))) {
            throw new DdlException("Provider must be one of " + PROVIDERS);
        }

        // Only the 'local' provider can ignore the 'api-key'
        if (!"LOCAL".equals(properties.get(AIProperties.PROVIDER_TYPE))
                && Strings.isNullOrEmpty(properties.get(AIProperties.API_KEY))) {
            throw new DdlException("Missing [" + API_KEY + "] in properties for provider: "
                    + properties.get(AIProperties.PROVIDER_TYPE));
        }

        // Check weather the 'temperature' is valid
        String temp = properties.get(AIProperties.TEMPERATURE);
        if (!Strings.isNullOrEmpty(temp) && !temp.equals("-1")) {
            double tempVal = Double.parseDouble(temp);
            if (!(tempVal >= 0 && tempVal <= 1)) {
                throw new DdlException("Temperature must be a double between 0 and 1");
            }
        }

        // Check 'dimensions'
        temp = properties.get(AIProperties.DIMENSIONS);
        if (!Strings.isNullOrEmpty(temp) && temp.equals("-1")) {
            int tempVal = Integer.parseInt(temp);
            if (tempVal <= 0) {
                throw new DdlException("Dimensions must be a positive integer");
            }
        }
    }

    public static void optionalAIProperties(Map<String, String> properties) {
        properties.putIfAbsent(AIProperties.TEMPERATURE, AIProperties.DEFAULT_TEMPERATURE);
        properties.putIfAbsent(AIProperties.MAX_TOKEN, AIProperties.DEFAULT_MAX_TOKEN);
        properties.putIfAbsent(AIProperties.MAX_RETRIES, AIProperties.DEFAULT_MAX_RETRIES);
        properties.putIfAbsent(AIProperties.RETRY_DELAY_SECOND, AIProperties.DEFAULT_RETRY_DELAY_SECOND);
        properties.putIfAbsent(AIProperties.ANTHROPIC_VERSION, AIProperties.DEFAULT_ANTHROPIC_VERSION);
        properties.putIfAbsent(AIProperties.DIMENSIONS, AIProperties.DEFAULT_DIMENSIONS);
    }
}
