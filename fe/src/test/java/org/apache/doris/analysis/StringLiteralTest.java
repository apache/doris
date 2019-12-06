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

package org.apache.doris.analysis;

import org.junit.Assert;
import org.junit.Test;

public class StringLiteralTest {
    @Test
    public void leadingNum() {
        Assert.assertEquals("123", StringLiteral.leadingNum("123"));
        Assert.assertEquals("", StringLiteral.leadingNum("abc"));
        Assert.assertEquals("123", StringLiteral.leadingNum("123abc"));
        Assert.assertEquals("+123", StringLiteral.leadingNum("+123"));
        Assert.assertEquals("-123", StringLiteral.leadingNum("-123"));
        Assert.assertEquals("-123", StringLiteral.leadingNum("-123abc"));
        Assert.assertEquals("123e123", StringLiteral.leadingNum("123e123"));
        Assert.assertEquals("123e+123", StringLiteral.leadingNum("123e+123"));
        Assert.assertEquals("123e-123", StringLiteral.leadingNum("123e-123"));
        Assert.assertEquals("123e1", StringLiteral.leadingNum("123e1-23"));
        Assert.assertEquals("1.23e123", StringLiteral.leadingNum("1.23e123abc"));
        Assert.assertEquals("1.23e1", StringLiteral.leadingNum("1.23e1"));
        Assert.assertEquals("1.2", StringLiteral.leadingNum("1.2.3e"));
        Assert.assertEquals("1", StringLiteral.leadingNum("1-23"));
        Assert.assertEquals("1", StringLiteral.leadingNum("1+3"));
        Assert.assertEquals("1", StringLiteral.leadingNum("1..22"));
        Assert.assertEquals("-.1", StringLiteral.leadingNum("-.1"));
        Assert.assertEquals("", StringLiteral.leadingNum("-."));
        Assert.assertEquals("", StringLiteral.leadingNum("-.bac"));
        Assert.assertEquals("123", StringLiteral.leadingNum("123e.abc"));
        Assert.assertEquals("123", StringLiteral.leadingNum("123e-.abc"));
    }
}


