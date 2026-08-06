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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test for null sessionVariables fix after upgrade from 4.0.2 to 4.0.4+
 * This test ensures that when objects are deserialized from old snapshots (without sessionVariables),
 * the gsonPostProcess method initializes sessionVariables to an empty HashMap to prevent NPE.
 */
public class SessionVariablesNullFixTest {

    /**
     * Test View with null sessionVariables after gsonPostProcess
     * Simulates upgrading from 4.0.2 where sessionVariables field did not exist
     */
    @Test
    public void testViewSessionVariablesNullInitialized() throws IOException {
        View view = new View();
        // Simulate old deserialized object where sessionVariables is null
        view.setSessionVariables(null);

        // gsonPostProcess should initialize it
        view.gsonPostProcess();

        Assert.assertNotNull(view.getSessionVariables());
        Assert.assertEquals(0, view.getSessionVariables().size());
        Assert.assertEquals("{}", view.getSessionVariables().toString());
    }

    /**
     * Test Column with null sessionVariables after gsonPostProcess
     */
    @Test
    public void testColumnSessionVariablesNullInitialized() throws IOException {
        Column column = new Column("test_col", Type.fromPrimitiveType(PrimitiveType.BIGINT));
        // Simulate old deserialized object where sessionVariables is null
        column.setSessionVariables(null);

        // gsonPostProcess should initialize it
        column.gsonPostProcess();

        Assert.assertNotNull(column.getSessionVariables());
        Assert.assertEquals(0, column.getSessionVariables().size());
        Assert.assertEquals("{}", column.getSessionVariables().toString());
    }

    /**
     * Test AliasFunction with null sessionVariables after gsonPostProcess
     */
    @Test
    public void testAliasFunctionSessionVariablesNullInitialized() throws IOException {
        AliasFunction aliasFunction = new AliasFunction(
                new FunctionName("test_alias"),
                new java.util.ArrayList<>(),
                Type.fromPrimitiveType(PrimitiveType.INT),
                false
        );
        // Simulate old deserialized object where sessionVariables is null
        aliasFunction.setSessionVariables(null);

        // gsonPostProcess should initialize it
        aliasFunction.gsonPostProcess();

        Assert.assertNotNull(aliasFunction.getSessionVariables());
        Assert.assertEquals(0, aliasFunction.getSessionVariables().size());
        Assert.assertEquals("{}", aliasFunction.getSessionVariables().toString());
    }



    /**
     * Test that sessionVariables toString() doesn't throw NPE
     * This is the actual bug that was reported in ShowCreateMTMVInfo
     */
    @Test
    public void testSessionVariablesToStringDoesNotThrowNPE() throws IOException {
        View view = new View();
        view.setSessionVariables(null);

        // Before fix: view.getSessionVariables().toString() would throw NPE
        // After fix with gsonPostProcess: should not throw
        view.gsonPostProcess();
        String result = view.getSessionVariables().toString();

        Assert.assertNotNull(result);
        Assert.assertEquals("{}", result);
    }

    /**
     * Test that existing sessionVariables are preserved during gsonPostProcess
     */
    @Test
    public void testSessionVariablesPreservedWhenNotNull() throws IOException {
        View view = new View();
        java.util.Map<String, String> vars = new java.util.HashMap<>();
        vars.put("key1", "value1");
        vars.put("key2", "value2");
        view.setSessionVariables(vars);

        view.gsonPostProcess();

        Assert.assertNotNull(view.getSessionVariables());
        Assert.assertEquals(2, view.getSessionVariables().size());
        Assert.assertEquals("value1", view.getSessionVariables().get("key1"));
        Assert.assertEquals("value2", view.getSessionVariables().get("key2"));
    }
}
