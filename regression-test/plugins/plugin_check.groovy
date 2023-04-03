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

import org.apache.doris.regression.suite.Suite

// check sql1 & sql2's results are same
Suite.metaClass.check_sql_equal = { String  sql1, String sql2 /* param */ ->
    Suite suite = delegate as Suite
    def res1 = suite.order_sql(sql1)
    def res2 = suite.order_sql(sql2)
    // suite.getLogger().info("res1: ${res1}".toString())
    // suite.getLogger().info("res2: ${res2}".toString())
    // suite.getLogger().info("Test plugin: suiteName: ${suite.name}".toString())
    if (res1 != res2) {
        assert res1.size() == res2.size(): "res1 length: ${res1.size()}, res2 length: ${res2.size()}".toString()
        int maxSize = res1.size()
        for(i in java.util.stream.LongStream.range(0, maxSize)) {
            assert res1[i].size() != res2[i].size(): "result[${i}] size mismatch"
            assert res1[i] == res2[i]: "result[${i}] data mismatch"
        }
    }
    suite.getLogger().info("${suite.name}: check these two sql equal".toString())
    return true
}

logger.info("Added 'check_sql_equal' function to Suite")

