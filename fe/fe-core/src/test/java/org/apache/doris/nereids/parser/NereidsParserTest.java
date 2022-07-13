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

import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class NereidsParserTest {

    @Test
    public void testParseMultiple() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT b FROM test;SELECT a FROM test;";
        List<LogicalPlan> logicalPlanList = nereidsParser.parseMultiple(sql);
        Assertions.assertEquals(2, logicalPlanList.size());
    }

    @Test
    public void testSingle() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT * FROM test;";
        Exception exceptionOccurred = null;
        try {
            nereidsParser.parseSingle(sql);
        } catch (Exception e) {
            exceptionOccurred = e;
            e.printStackTrace();
        }
        Assertions.assertNull(exceptionOccurred);
    }

    @Test
    public void testErrorListener() {
        Exception exception = Assertions.assertThrows(ParseException.class, () -> {
            String sql = "select * from t1 where a = 1 illegal_symbol";
            NereidsParser nereidsParser = new NereidsParser();
            nereidsParser.parseSingle(sql);
        });
        Assertions.assertEquals("\nextraneous input 'illegal_symbol' expecting {<EOF>, ';'}(line 1, pos29)\n",
                exception.getMessage());
    }

    @Test
    public void testPostProcessor() {
        String sql = "select `AD``D` from t1 where a = 1";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        LogicalProject<Plan> logicalProject = (LogicalProject) logicalPlan;
        Assertions.assertEquals("AD`D", logicalProject.getProjects().get(0).getName());
    }
}
