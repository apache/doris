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

suite("test_create_llm_resource") {
    String resourceName = 'test_create_llm_resource'

    //cloud-mode
    if (isCloudMode()) {
        return
    }

    try_sql("""DROP RESOURCE '${resourceName}'""")

    //If 'llm.validity_check'='false' is not set,
    // llm resource availability must be checked when creating the resource.

    // missing end_point
    test {
        sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'llm',
                'llm.provider_type' = 'deepseek',
                'llm.model_name' = 'deepseek-chat',
                'llm.api_key' = 'sk-xxx'
            );"""
        exception "Missing [llm.endpoint] in properties."
    }
    
    // missing provider_type
    test {
        sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'llm',
                'llm.endpoint' = 'https://api.deepseek.com/chat/completions',
                'llm.model_name' = 'deepseek-chat',
                'llm.api_key' = 'sk-xxx'
            );"""
        exception "Missing [llm.provider_type] in properties."
    }

    // missing model_name
    test {
        sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'llm',
                'llm.provider_type' = 'deepseek',
                'llm.endpoint' = 'https://api.deepseek.com/chat/completions',
                'llm.api_key' = 'sk-xxx'
            );"""
        exception "Missing [llm.model_name] in properties."
    }

    // missing api-key while provider_type is not local
    test {
        sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'llm',
                'llm.provider_type' = 'deepseek',
                'llm.endpoint' = 'https://api.deepseek.com/chat/completions',
                'llm.model_name' = 'deepseek-chat'
            );"""
        exception "Missing [llm.api_key] in properties for provider: DEEPSEEK"
    }

    sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'llm',
                'llm.provider_type' = 'deepseek',
                'llm.endpoint' = 'https://api.deepseek.com/chat/completions',
                'llm.model_name' = 'deepseek-chat',
                'llm.api_key' = 'sk-xxx',
                'llm.temperature' = '0.7',
                'llm.max_token' = '1024',
                'llm.max_retries' = '3',
                'llm.retry_delay_second' = '1',
                'llm.validity_check' = 'false'
            );"""
    def res = sql """SHOW RESOURCES WHERE NAME = '${resourceName}'"""
    assertTrue(res.size() > 0)

    try_sql("""DROP RESOURCE '${resourceName}'""")
}