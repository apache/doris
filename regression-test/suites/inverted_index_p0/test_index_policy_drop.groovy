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

suite("test_index_policy_drop") {
    def tokenizerName = "test_tokenizer_drop_check"

    // 1. Create a tokenizer policy
    sql "DROP INVERTED INDEX TOKENIZER IF EXISTS ${tokenizerName}"
    sql """
        CREATE INVERTED INDEX TOKENIZER ${tokenizerName}
        PROPERTIES("type" = "pinyin")
    """

    // 2. Attempt to drop it using DROP ANALYZER syntax (should fail)
    test {
        sql "DROP INVERTED INDEX ANALYZER ${tokenizerName}"
        exception "Cannot drop TOKENIZER policy '${tokenizerName}' by DROP ANALYZER statement"
    }

    // 3. Drop it using correct syntax (should succeed)
    sql "DROP INVERTED INDEX TOKENIZER ${tokenizerName}"
}
