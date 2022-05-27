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

import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

public class HelpStmtTest {
    @Test
    public void testNormal() throws AnalysisException {
        HelpStmt stmt = new HelpStmt("contents");
        stmt.analyze(null);
        Assert.assertEquals("contents", stmt.getMask());
        Assert.assertEquals("HELP contents", stmt.toString());

        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals(3, stmt.getCategoryMetaData().getColumnCount());
        Assert.assertEquals(2, stmt.getKeywordMetaData().getColumnCount());
    }

    @Test(expected = AnalysisException.class)
    public void testEmpty() throws AnalysisException {
        HelpStmt stmt = new HelpStmt("");
        stmt.analyze(null);
        Assert.fail("No exception throws.");
    }
}
