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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Type;

import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;

public class PaimonUtilTest {
    @Test
    public void testSchemaForVarcharAndChar() {
        DataField c1 = new DataField(1, "c1", new VarCharType(32));
        DataField c2 = new DataField(2, "c2", new CharType(14));
        Type type1 = PaimonUtil.paimonTypeToDorisType(c1.type());
        Type type2 = PaimonUtil.paimonTypeToDorisType(c2.type());
        Assert.assertTrue(type1.isVarchar());
        Assert.assertEquals(32, type1.getLength());
        Assert.assertEquals(14, type2.getLength());
    }
}
