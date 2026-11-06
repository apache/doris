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

suite("test_create_ai_resource") {
    String resourceName = 'test_create_ai_resource'

    //cloud-mode
    if (isCloudMode()) {
        return
    }

    try_sql("""DROP RESOURCE '${resourceName}'""")

    //If 'ai.validity_check'='false' is not set,
    // ai resource availability must be checked when creating the resource.

    // missing end_point
    test {
        sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'ai',
                'ai.provider_type' = 'deepseek',
                'ai.model_name' = 'deepseek-chat',
                'ai.api_key' = 'sk-xxx'
            );"""
        exception "Missing [ai.endpoint] in properties."
    }
    
    // missing provider_type
    test {
        sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'ai',
                'ai.endpoint' = 'https://api.deepseek.com/chat/completions',
                'ai.model_name' = 'deepseek-chat',
                'ai.api_key' = 'sk-xxx'
            );"""
        exception "Missing [ai.provider_type] in properties."
    }

    // missing model_name
    test {
        sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'ai',
                'ai.provider_type' = 'deepseek',
                'ai.endpoint' = 'https://api.deepseek.com/chat/completions',
                'ai.api_key' = 'sk-xxx'
            );"""
        exception "Missing [ai.model_name] in properties."
    }

    // missing api-key while provider_type is not local
    test {
        sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'ai',
                'ai.provider_type' = 'deepseek',
                'ai.endpoint' = 'https://api.deepseek.com/chat/completions',
                'ai.model_name' = 'deepseek-chat'
            );"""
        exception "Missing [ai.api_key] in properties for provider: DEEPSEEK"
    }

    for (String key : ['ai.api_key', 'ai.max_retries', 'ai.anthropic_version', 'ai.effort']) {
        for (String value : ['', '   ']) {
            test {
                sql """CREATE RESOURCE "${resourceName}" PROPERTIES(
                    'type' = 'ai',
                    'ai.provider_type' = 'local',
                    'ai.endpoint' = 'http://localhost:8000/v1/chat/completions',
                    'ai.model_name' = 'chat-model',
                    '${key}' = '${value}'
                );"""
                exception "[${key}] must not be empty"
            }
        }
    }

    def invalidNumericProperties = [
        ['ai.temperature', 'NaN'],
        ['ai.temperature', 'Infinity'],
        ['ai.temperature', '-Infinity'],
        ['ai.temperature', '-0.1'],
        ['ai.temperature', '1.1'],
        ['ai.max_token', '0'],
        ['ai.max_token', '-2'],
        ['ai.max_token', '9223372036854775808'],
        ['ai.max_retries', '-1'],
        ['ai.max_retries', '2147483648'],
        ['ai.retry_delay_second', '-1'],
        ['ai.retry_delay_second', '2147483648'],
        ['ai.dimensions', '0'],
        ['ai.dimensions', '-2'],
        ['ai.dimensions', '2147483648']
    ]
    for (def entry : invalidNumericProperties) {
        test {
            sql """CREATE RESOURCE "${resourceName}" PROPERTIES(
                'type' = 'ai',
                'ai.provider_type' = 'local',
                'ai.endpoint' = 'http://localhost:8000/v1/chat/completions',
                'ai.model_name' = 'chat-model',
                '${entry[0]}' = '${entry[1]}'
            );"""
            exception "Failed to set property '${entry[0]}'"
        }
    }

    sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'ai',
                'ai.provider_type' = ' deepseek ',
                'ai.endpoint' = ' https://api.deepseek.com/chat/completions ',
                'ai.model_name' = ' deepseek-chat ',
                'ai.api_key' = ' sk-xxx ',
                'ai.temperature' = ' 0.7 ',
                'ai.max_token' = ' 1024 ',
                'ai.max_retries' = ' 3 ',
                'ai.retry_delay_second' = ' 1 ',
                'ai.dimensions' = ' 768 ',
                'ai.anthropic_version' = ' 2023-06-01 ',
                'ai.effort' = ' high ',
                'ai.enable_structured_output' = ' true ',
                'ai.validity_check' = 'false'
            );"""
    def res = sql """SHOW RESOURCES WHERE NAME = '${resourceName}'"""
    assertTrue(res.size() > 0)
    def checkStoredProperties = { tag ->
        quickRunTest(tag, "SHOW RESOURCES WHERE NAME = '${resourceName}'", true) { row ->
            [row[2], row[2] in ['id', 'version'] ? '<generated>' : row[3]]
        }
    }
    checkStoredProperties("normalized_create")

    for (String key : ['ai.model_name', 'ai.api_key', 'ai.max_retries', 'ai.anthropic_version', 'ai.effort']) {
        for (String value : ['', '   ']) {
            test {
                sql """ALTER RESOURCE "${resourceName}" PROPERTIES('${key}' = '${value}');"""
                exception "[${key}] must not be empty"
            }
        }
    }

    for (def entry : invalidNumericProperties) {
        test {
            sql """ALTER RESOURCE "${resourceName}" PROPERTIES('${entry[0]}' = '${entry[1]}');"""
            exception "Failed to set property '${entry[0]}'"
        }
    }

    sql """ALTER RESOURCE "${resourceName}" PROPERTIES(
        'ai.temperature' = '1', 'ai.max_token' = '9223372036854775807',
        'ai.max_retries' = '2147483647', 'ai.retry_delay_second' = '2147483647',
        'ai.dimensions' = '2147483647'
    );"""
    sql """ALTER RESOURCE "${resourceName}" PROPERTIES(
        'ai.temperature' = '0', 'ai.max_token' = '1',
        'ai.max_retries' = '0', 'ai.retry_delay_second' = '0', 'ai.dimensions' = '1'
    );"""
    sql """ALTER RESOURCE "${resourceName}" PROPERTIES(
        'ai.temperature' = '-1', 'ai.max_token' = '-1', 'ai.dimensions' = '-1'
    );"""
    sql """ALTER RESOURCE "${resourceName}" PROPERTIES(
        'ai.temperature' = ' 0.5 ', 'ai.max_token' = ' 2048 ',
        'ai.max_retries' = ' 4 ', 'ai.retry_delay_second' = ' 2 ',
        'ai.dimensions' = ' 1024 ', 'ai.model_name' = ' updated-model ',
        'ai.enable_structured_output' = ' false '
    );"""
    checkStoredProperties("normalized_alter")

    try_sql("""DROP RESOURCE '${resourceName}'""")
}
