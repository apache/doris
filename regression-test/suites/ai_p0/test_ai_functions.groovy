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

suite("test_ai_functions") {
    String resourceName = 'test_ai_functions_resource'
    String summarize_query = "SELECT AI_SUMMARIZE('this is a test');"
    String sentiment_query = "SELECT AI_SENTIMENT('this is a test');"

    //cloud-mode
    if (isCloudMode()) {
        return
    }

    try_sql("""SET default_ai_resource = default;""")
    try_sql("""DROP RESOURCE '${resourceName}'""")
    sql """CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                'type' = 'ai',
                'ai.provider_type' = 'deepseek',
                'ai.endpoint' = 'https://api.deepseek.com/chat/completions',
                'ai.model_name' = 'deepseek-chat',
                'ai.api_key' = 'sk-xxx',
                'ai.temperature' = '0.7',
                'ai.max_token' = '1024',
                'ai.max_retries' = '2',
                'ai.retry_delay_second' = '3',
                'ai.validity_check' = 'false'
            );"""
    
    def res = sql """SHOW RESOURCES WHERE NAME = '${resourceName}'"""
    assertTrue(res.size() > 0)

    test {
        sql """${summarize_query}"""
        exception "Can not build function: 'AI_SUMMARIZE', expression: AI_SUMMARIZE('this is a test'), Please specify the AI Resource in argument or session variable."

    }

    String test_table_for_ai_functions = "test_table_for_ai_functions"
    String query_with_not_const_resource = "SELECT AI_TRANSLATE(resource_name, text, tar_lag) FROM ${test_table_for_ai_functions};"

    try_sql("""DROP TABLE IF EXISTS ${test_table_for_ai_functions}""")
    sql """CREATE TABLE IF NOT EXISTS ${test_table_for_ai_functions} (
            resource_name VARCHAR(100),
            text VARCHAR(100),
            tar_lag VARCHAR(100),
            label ARRAY<VARCHAR(100)>
        )
        DUPLICATE KEY(resource_name)
        DISTRIBUTED BY HASH(resource_name) BUCKETS 1
        PROPERTIES("replication_num" = "1");"""

    sql """INSERT INTO ${test_table_for_ai_functions}(resource_name, text, tar_lag, label)
            VALUES ('${resourceName}', 'this is a test', 'zh-CN', ['label']),
            ('${resourceName}', NULL, 'zh-CN', NULL);"""

    // the ai resource must be literal
    test {
        sql """${query_with_not_const_resource}"""
        exception "AI Function must accept literal for the resource name."
    }

    // for AI_agg, the task(third arg) must be literal
    test {
        sql """SELECT AI_AGG('${resourceName}', text, tar_lag) FROM ${test_table_for_ai_functions};"""
        exception "AI_AGG must accept literal for the task."
    }

    // Test for normal call
    // test the default resource
    try {
        sql """set query_timeout=2;"""
        sql """set default_ai_resource='${resourceName}';"""
        test {
            sql """${sentiment_query}"""
            exception "timeout when waiting for send fragments rpc, query timeout:2"
        }
    } finally {
        sql """UNSET VARIABLE query_timeout;"""
        sql """UNSET VARIABLE default_ai_resource;"""
    }

    def test_query_timeout_exception = { sql_text ->
        try {
            sql """set query_timeout=1;"""
            test {
                sql """${sql_text}"""
                exception "timeout"
            }
        } finally {
            sql """UNSET VARIABLE query_timeout;"""
        }
    }

    test_query_timeout_exception("SELECT AI_TRANSLATE('${resourceName}', text, 'zh-CN') FROM ${test_table_for_ai_functions};")
    test_query_timeout_exception("SELECT AI_CLASSIFY('${resourceName}', text, label) FROM ${test_table_for_ai_functions};")
    test_query_timeout_exception("SELECT AI_EXTRACT('${resourceName}', 'this is a test', ['task']) FROM ${test_table_for_ai_functions};")
    test_query_timeout_exception("SELECT AI_FIXGRAMMAR('${resourceName}', text) FROM ${test_table_for_ai_functions};")
    test_query_timeout_exception("SELECT AI_GENERATE('${resourceName}', 'generate something');")
    test_query_timeout_exception("SELECT AI_SUMMARIZE('${resourceName}', 'test,test,test,test')")
    test_query_timeout_exception("SELECT AI_SENTIMENT('${resourceName}', 'this is a test');")
    test_query_timeout_exception("SELECT AI_MASK('${resourceName}', 'this is a test', label) FROM ${test_table_for_ai_functions};")
    test_query_timeout_exception("SELECT AI_FILTER('${resourceName}', text) FROM ${test_table_for_ai_functions};")
    test_query_timeout_exception("SELECT AI_SIMILARITY('${resourceName}', 'this is a similarity test', text) FROM ${test_table_for_ai_functions};")
    test_query_timeout_exception("SELECT AI_AGG('${resourceName}', text, 'this is a test') FROM ${test_table_for_ai_functions};")

    String embedResourceName = "embedResourceName"
    try_sql("""DROP RESOURCE IF EXISTS '${embedResourceName}'""")
    sql """CREATE RESOURCE 'embedResourceName'
            PROPERTIES (
            'type'='ai',
            'ai.provider_type'='qwen',
            'ai.endpoint'='https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings',
            'ai.model_name' = 'text-embedding-v4',
            'ai.api_key' = 'sk-xxxx',
            'ai.dimensions' = '1024'
        );"""
    
    res = sql """SHOW RESOURCES WHERE NAME = '${embedResourceName}'"""
    assertTrue(res.size() > 0)

    test_query_timeout_exception("SELECT EMBED('${embedResourceName}', text) FROM ${test_table_for_ai_functions};")

    try_sql("""DROP TABLE IF EXISTS ${test_table_for_ai_functions}""")
    try_sql("""DROP RESOURCE IF EXISTS '${resourceName}'""")
    try_sql("""DROP RESOURCE IF EXISTS '${embedResourceName}'""")
}
