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

package org.apache.doris.catalog;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.Function.CompareMode;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class FunctionSetTest {

    private FunctionSet functionSet;

    @Before
    public void setUp() {
        functionSet = new FunctionSet();
        functionSet.init();
    }

    @Test
    public void testGetLagFunction() {
        Type[] argTypes1 = {ScalarType.DECIMALV2, ScalarType.TINYINT, ScalarType.TINYINT};
        Function lagDesc1 = new Function(new FunctionName("lag"), Arrays.asList(argTypes1), ScalarType.INVALID, false);
        Function newFunction = functionSet.getFunction(lagDesc1, Function.CompareMode.IS_SUPERTYPE_OF);
        Type[] newArgTypes = newFunction.getArgs();
        Assert.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assert.assertTrue(newArgTypes[0].matchesType(ScalarType.DOUBLE));

        Type[] argTypes2 = {ScalarType.VARCHAR, ScalarType.TINYINT, ScalarType.TINYINT};
        Function lagDesc2 = new Function(new FunctionName("lag"), Arrays.asList(argTypes2), ScalarType.INVALID, false);
        newFunction = functionSet.getFunction(lagDesc2, Function.CompareMode.IS_SUPERTYPE_OF);
        newArgTypes = newFunction.getArgs();
        Assert.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assert.assertTrue(newArgTypes[0].matchesType(ScalarType.VARCHAR));
    }

    @Test
    public void testAddInferenceFunction() {
        TemplateType type1 = new TemplateType("T");
        TemplateType type2 = new TemplateType("T");
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltin(
                "test_a", Type.ANY_ELEMENT_TYPE, Lists.newArrayList(type1, type2), false,
                "", "", "", true));
        Type[] argTypes = {ArrayType.create(), ScalarType.INT};
        Function desc = new Function(new FunctionName("test_a"), Arrays.asList(argTypes), ScalarType.INVALID, false);
        Function result = functionSet.getFunction(desc, CompareMode.IS_IDENTICAL);
        Assert.assertNull(result);
    }

}
