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

package org.apache.doris.mysql.authenticate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AuthenticationChainFallbackPolicyTest {
    @Test
    void testFromConfigParsesKnownPolicies() {
        Assertions.assertEquals(AuthenticationChainFallbackPolicy.DISABLED,
                AuthenticationChainFallbackPolicy.fromConfig(null));
        Assertions.assertEquals(AuthenticationChainFallbackPolicy.DISABLED,
                AuthenticationChainFallbackPolicy.fromConfig("disabled"));
        Assertions.assertEquals(AuthenticationChainFallbackPolicy.USER_NOT_FOUND,
                AuthenticationChainFallbackPolicy.fromConfig("user_not_found"));
        Assertions.assertEquals(AuthenticationChainFallbackPolicy.ANY_FAILURE,
                AuthenticationChainFallbackPolicy.fromConfig("any_failure"));
        Assertions.assertEquals(AuthenticationChainFallbackPolicy.DISABLED,
                AuthenticationChainFallbackPolicy.fromConfig("unknown"));
    }
}
