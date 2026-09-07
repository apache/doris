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


import org.apache.doris.regression.action.ProfileAction
import org.apache.doris.regression.suite.Suite
import java.util.regex.Pattern

// Execution-path assertions for UUID regression suites. SQL result expectations stay in .out.
// A unique SQL comment binds the complete profile to the exact query under test. Only positive
// versus zero is asserted: merged profiles may contain both summary and per-instance counters.
Suite.metaClass.uuidCheckProfile = { String token, List<String> positive, List<String> zero ->
    Suite suite = delegate as Suite
    List<String> names = (positive + zero).unique()
    String profile = new ProfileAction(suite.context).getProfileBySql(token, names)
    names.each { String name ->
        def matches = (profile =~ /(?m)^\s*-\s*${Pattern.quote(name)}:\s*([0-9,.]+)/)
        List<BigDecimal> values = matches.collect { new BigDecimal(it[1].replace(',', '')) }
        if (values.isEmpty()) {
            throw new IllegalStateException("Missing UUID profile counter ${name}: ${profile}")
        }
        boolean active = values.any { it > 0 }
        if (active != positive.contains(name)) {
            throw new IllegalStateException("UUID ${token}: ${name} expected "
                    + (positive.contains(name) ? 'positive' : 'zero') + ", got ${values}: ${profile}")
        }
    }
    return profile
}
