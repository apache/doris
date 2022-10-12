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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;

import org.junit.Assert;
import org.junit.Test;

public class CastExprTest {
    @Test
    public void testCastTo() {
        ScalarType charType = ScalarType.createType(PrimitiveType.CHAR, 2, 0, 0);
        ScalarType varcharType = ScalarType.createType(PrimitiveType.VARCHAR, 2, 0, 0);
        ScalarType stringType = ScalarType.createType(PrimitiveType.STRING, 2, 0, 0);

        StringLiteral value = new StringLiteral("20210926");
        String subStringValue1 = value.getStringValue().substring(0, charType.getLength());
        Assert.assertEquals(subStringValue1, "20");
        String subStringValue2 = value.getStringValue().substring(0, varcharType.getLength());
        Assert.assertEquals(subStringValue2, "20");
        String subStringValue3 = value.getStringValue().substring(0, stringType.getLength());
        Assert.assertEquals(subStringValue3, "20");

    }
}

