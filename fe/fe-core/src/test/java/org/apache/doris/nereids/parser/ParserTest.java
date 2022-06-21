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
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParserTest {
    @Test
    public void testErrorListener() {
        Exception exception = Assertions.assertThrows(ParseException.class, () -> {
            String sql = "select * from t1 where a = 1 illegal_symbol";
            SqlParser sqlParser = new SqlParser();
            sqlParser.parse(sql);
        });
        Assertions.assertEquals("\nextraneous input 'illegal_symbol' expecting {<EOF>, ';'}(line 1, pos29)\n",
                exception.getMessage());
    }

    @Test
    public void testPostProcessor() {
        String sql = "select `AD``D` from t1 where a = 1";
        SqlParser sqlParser = new SqlParser();
        LogicalPlan logicalPlan = sqlParser.parse(sql);
        LogicalProject logicalProject = (LogicalProject) logicalPlan.getOperator();
        Assertions.assertEquals("AD`D", logicalProject.getProjects().get(0).getName());
    }
}
