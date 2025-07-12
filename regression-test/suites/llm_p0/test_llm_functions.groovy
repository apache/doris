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

import org.junit.Assert;

suite("test_llm_functions") {
    String resourceName = 'test_llm_functions_resource'
    String summarize_query = "SELECT LLM_SUMMARIZE('this is a test');"
    String sentiment_query = "SELECT LLM_SENTIMENT('this is a test');"

    //cloud-mode
    if (isCloudMode()) {
        return
    }

    try_sql("""DROP RESOURCE '${resourceName}'""")

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
                'llm.retry_delay_ms' = '1000',
                'llm.timeout_ms' = '30000',
                'llm.validity_check' = 'false'
            );"""
    
    def res = sql """SHOW RESOURCES WHERE NAME = '${resourceName}'"""
    assertTrue(res.size() > 0)

    test {
        sql """${summarize_query}"""
        exception "Can not build function: 'LLM_SUMMARIZE', expression: LLM_SUMMARIZE('this is a test'), Please specify the LLM Resource in argument or session variable."

    }

    // Test a specific function resource
    try {
        sql """set query_timeout=5;"""
        sql """set default_llm_summarize_resource='${resourceName}';"""
        test {
            sql """${summarize_query}"""
            exception "timeout when waiting for send fragments rpc, query timeout:5"
        }
    } finally {
        sql """UNSET VARIABLE query_timeout;"""
        sql """UNSET VARIABLE default_llm_summarize_resource;"""
    }

    // test the default resource
    try {
        sql """set query_timeout=5;"""
        sql """set default_llm_resource='${resourceName}';"""
        test {
            sql """${sentiment_query}"""
            exception "timeout when waiting for send fragments rpc, query timeout:5"
        }
    } finally {
        sql """UNSET VARIABLE query_timeout;"""
        sql """UNSET VARIABLE default_llm_resource;"""
    }

    try_sql("""DROP RESOURCE IF EXISTS '${resourceName}'""")
}