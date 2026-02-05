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

package org.apache.doris.nereids.trees.expressions.literal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IPV6LiteralTest {

    @Test
    public void testValidIPv6() {
        Assertions.assertTrue(IPv6Literal.isValidIPv6("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
        Assertions.assertTrue(IPv6Literal.isValidIPv6("fe80::1"));
        Assertions.assertTrue(IPv6Literal.isValidIPv6("2001:db8::8a2e:370:7334"));
        Assertions.assertTrue(IPv6Literal.isValidIPv6("::1"));
        Assertions.assertTrue(IPv6Literal.isValidIPv6("::ffff:192.168.1.1"));
        Assertions.assertTrue(IPv6Literal.isValidIPv6("::FFFF:10.0.0.255"));
    }

    @Test
    public void testInvalidIPv6() {
        Assertions.assertFalse(IPv6Literal.isValidIPv6("2001:db8::8a2e::370"));
        Assertions.assertFalse(IPv6Literal.isValidIPv6("2001:db8:xyz::1"));
        Assertions.assertFalse(IPv6Literal.isValidIPv6("192.168.1.1"));
        Assertions.assertFalse(IPv6Literal.isValidIPv6("::aaaa:192.168.1.1"));
        Assertions.assertFalse(IPv6Literal.isValidIPv6("::FFFF:10.0.0.256"));
    }
}
