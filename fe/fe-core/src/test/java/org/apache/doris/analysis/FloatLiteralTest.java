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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;

import org.junit.Assert;
import org.junit.Test;


public class FloatLiteralTest {
    @Test
    public void testGetStringInFe() throws AnalysisException {
        FloatLiteral literal = new FloatLiteral((double) (11 * 3600 + 22 * 60 + 33),
                FloatLiteral.getDefaultTimeType(Type.TIME));
        String s = literal.getStringValueInFe(FormatOptions.getDefault());
        Assert.assertEquals("11:22:33", s);
        Assert.assertEquals("11:22:33", literal.getStringValueInFe(FormatOptions.getForPresto()));

        FloatLiteral literal1 = new FloatLiteral(11.22);
        String s1 = literal1.getStringValueInFe(FormatOptions.getDefault());
        Assert.assertEquals("11.22", s1);
        Assert.assertEquals("11.22", literal1.getStringValueInFe(FormatOptions.getForPresto()));
    }
}
