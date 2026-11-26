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

import java.util.Arrays;
import java.util.List;

public class AIProperties extends BaseProperties {
    public static final String AI_PREFIX = "ai.";

    // required
    public static final String ENDPOINT = "ai.endpoint";
    public static final String PROVIDER_TYPE = "ai.provider_type";
    public static final String MODEL_NAME = "ai.model_name";
    public static final String EMBED_ENDPOINT = "ai.embed.endpoint";
    public static final String EMBED_PROVIDER_TYPE = "ai.embed.provider_type";
    public static final String EMBED_MODEL_NAME = "ai.embed.model_name";
    public static final String MULTIMODAL_EMBED_ENDPOINT = "ai.embed.mm.endpoint";
    public static final String MULTIMODAL_EMBED_PROVIDER_TYPE = "ai.embed.mm.provider_type";
    public static final String MULTIMODAL_EMBED_MODEL_NAME = "ai.embed.mm.model_name";

    // optional
    public static final String API_KEY = "ai.api_key";
    public static final String EMBED_API_KEY = "ai.embed.api_key";
    public static final String MULTIMODAL_EMBED_API_KEY = "ai.embed.mm.api_key";
    public static final String TEMPERATURE = "ai.temperature";
    public static final String MAX_TOKEN = "ai.max_token";
    public static final String MAX_RETRIES = "ai.max_retries";
    public static final String RETRY_DELAY_SECOND = "ai.retry_delay_second";
    public static final String ANTHROPIC_VERSION = "ai.anthropic_version";
    public static final String DIMENSIONS = "ai.dimensions";
    public static final String EFFORT = "ai.effort";

    public static final String ENABLE_STRUCTURED_OUTPUT = "ai.enable_structured_output";

    // default_val
    public static final String DEFAULT_ENABLE_STRUCTURED_OUTPUT = "true";
    public static final String DEFAULT_TEMPERATURE = "-1";
    public static final String DEFAULT_MAX_TOKEN = "-1";
    public static final String DEFAULT_MAX_RETRIES = "3";
    public static final String DEFAULT_RETRY_DELAY_SECOND = "1";
    public static final String DEFAULT_ANTHROPIC_VERSION = "2023-06-01";
    public static final String DEFAULT_DIMENSIONS = "-1";

    public static final String VALIDITY_CHECK = "ai.validity_check";

    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, PROVIDER_TYPE, MODEL_NAME);
    public static final List<String> EMBED_REQUIRED_FIELDS =
            Arrays.asList(EMBED_ENDPOINT, EMBED_PROVIDER_TYPE, EMBED_MODEL_NAME);
    public static final List<String> MULTIMODAL_EMBED_REQUIRED_FIELDS =
            Arrays.asList(MULTIMODAL_EMBED_ENDPOINT, MULTIMODAL_EMBED_PROVIDER_TYPE,
                    MULTIMODAL_EMBED_MODEL_NAME);
    public static final List<String> PROVIDERS
            = Arrays.asList("OPENAI", "LOCAL", "GEMINI", "DEEPSEEK", "ANTHROPIC",
            "MOONSHOT", "QWEN", "MINIMAX", "ZHIPU", "BAICHUAN", "VOYAGEAI", "JINA");
}
