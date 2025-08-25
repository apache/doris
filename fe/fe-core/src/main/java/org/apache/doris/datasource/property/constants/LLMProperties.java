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

public class LLMProperties extends BaseProperties {
    public static final String LLM_PREFIX = "llm.";

    // required
    public static final String ENDPOINT = "llm.endpoint";
    public static final String PROVIDER_TYPE = "llm.provider_type";
    public static final String MODEL_NAME = "llm.model_name";

    // optional
    public static final String API_KEY = "llm.api_key";
    public static final String TEMPERATURE = "llm.temperature";
    public static final String MAX_TOKEN = "llm.max_token";
    public static final String MAX_RETRIES = "llm.max_retries";
    public static final String RETRY_DELAY_SECOND = "llm.retry_delay_second";
    public static final String ANTHROPIC_VERSION = "llm.anthropic_version";
    public static final String DIMENSIONS = "llm.dimensions";

    // default_val
    public static final String DEFAULT_TEMPERATURE = "-1";
    public static final String DEFAULT_MAX_TOKEN = "-1";
    public static final String DEFAULT_MAX_RETRIES = "3";
    public static final String DEFAULT_RETRY_DELAY_SECOND = "0";
    public static final String DEFAULT_ANTHROPIC_VERSION = "2023-06-01";
    public static final String DEFAULT_DIMENSIONS = "-1";

    public static final String VALIDITY_CHECK = "llm.validity_check";

    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, PROVIDER_TYPE, MODEL_NAME);
    public static final List<String> PROVIDERS
            = Arrays.asList("OPENAI", "LOCAL", "GEMINI", "DEEPSEEK", "ANTHROPIC",
            "MOONSHOT", "QWEN", "MINIMAX", "ZHIPU", "BAICHUAN", "VOYAGEAI");

    public static void requiredLLMProperties(Map<String, String> properties) throws DdlException {
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
        if (!"LOCAL".equals(properties.get(LLMProperties.PROVIDER_TYPE))
                && Strings.isNullOrEmpty(properties.get(LLMProperties.API_KEY))) {
            throw new DdlException("Missing [" + API_KEY + "] in properties for provider: "
                    + properties.get(LLMProperties.PROVIDER_TYPE));
        }

        // Check weather the 'temperature' is valid
        String temp = properties.get(LLMProperties.TEMPERATURE);
        if (!Strings.isNullOrEmpty(temp) && !temp.equals("-1")) {
            double tempVal = Double.parseDouble(temp);
            if (!(tempVal >= 0 && tempVal <= 1)) {
                throw new DdlException("Temperature must be a double between 0 and 1");
            }
        }

        // Check 'dimensions'
        temp = properties.get(LLMProperties.DIMENSIONS);
        if (!Strings.isNullOrEmpty(temp) && temp.equals("-1")) {
            int tempVal = Integer.parseInt(temp);
            if (tempVal <= 0) {
                throw new DdlException("Dimensions must be a positive integer");
            }
        }
    }

    public static void optionalLLMProperties(Map<String, String> properties) {
        properties.putIfAbsent(LLMProperties.TEMPERATURE, LLMProperties.DEFAULT_TEMPERATURE);
        properties.putIfAbsent(LLMProperties.MAX_TOKEN, LLMProperties.DEFAULT_MAX_TOKEN);
        properties.putIfAbsent(LLMProperties.MAX_RETRIES, LLMProperties.DEFAULT_MAX_RETRIES);
        properties.putIfAbsent(LLMProperties.RETRY_DELAY_SECOND, LLMProperties.DEFAULT_RETRY_DELAY_SECOND);
        properties.putIfAbsent(LLMProperties.ANTHROPIC_VERSION, LLMProperties.DEFAULT_ANTHROPIC_VERSION);
        properties.putIfAbsent(LLMProperties.DIMENSIONS, LLMProperties.DEFAULT_DIMENSIONS);
    }
}
