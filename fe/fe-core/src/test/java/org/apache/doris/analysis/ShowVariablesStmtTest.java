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

public class ShowVariablesStmtTest {
    @Test
    public void testNormal() {
        ShowVariablesStmt stmt = new ShowVariablesStmt(null, null);
        stmt.analyze(null);
        Assert.assertEquals("SHOW DEFAULT VARIABLES", stmt.toString());
        Assert.assertEquals(4, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Variable_name", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Value", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("Default_Value", stmt.getMetaData().getColumn(2).getName());
        Assert.assertEquals("Changed", stmt.getMetaData().getColumn(3).getName());
        Assert.assertNull(stmt.getPattern());
        Assert.assertEquals(SetType.DEFAULT, stmt.getType());

        stmt = new ShowVariablesStmt(SetType.GLOBAL, "abc");
        stmt.analyze(null);
        Assert.assertEquals("SHOW GLOBAL VARIABLES LIKE 'abc'", stmt.toString());
        Assert.assertEquals(4, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Variable_name", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Value", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("abc", stmt.getPattern());
        Assert.assertEquals(SetType.GLOBAL, stmt.getType());
    }
}
