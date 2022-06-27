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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class NereidsParserTest {

    @Test
    public void testParseMultiple() throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT b FROM test;SELECT a FROM test;";
        List<LogicalPlan> logicalPlanList = nereidsParser.parseMultiple(sql);
        Assertions.assertEquals(2, logicalPlanList.size());
    }

    @Test
    public void testSingle() throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT * FROM test;";
        Exception exceptionOccurred = null;
        try {
            nereidsParser.parseSingle(sql);
        } catch (Exception e) {
            exceptionOccurred = e;
            e.printStackTrace();
        }
        Assert.assertNull(exceptionOccurred);
    }
}
