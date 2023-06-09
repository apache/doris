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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.catalog.Env;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UdfTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
    }
    
    @Test
    public void testSaveFunctionToRegistry() throws Exception {
        createFunction("create global alias function f(int) with parameter(n) as hours_add(now(3), n)");
        Assertions.assertEquals(1, Env.getCurrentEnv().getFunctionRegistry()
                .findUdfBuilder(null, "f").size());
        
        createFunction("create alias function f(int) with parameter(n) as hours_add(now(3), n)");
        Assertions.assertEquals(1, Env.getCurrentEnv().getFunctionRegistry()
                .findUdfBuilder("default_cluster:test", "f").size());
    }
}
