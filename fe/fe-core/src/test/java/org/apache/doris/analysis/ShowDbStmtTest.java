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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

public class ShowDbStmtTest {
    @Test
    public void testNormal() throws UserException, AnalysisException  {
        final Analyzer analyzer =  AccessTestUtil.fetchBlockAnalyzer();
        ShowDbStmt stmt = new ShowDbStmt(null);
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getPattern());
        Assert.assertEquals("SHOW DATABASES", stmt.toString());
        Assert.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());

        stmt = new ShowDbStmt("abc");
        stmt.analyze(analyzer);
        Assert.assertEquals("abc", stmt.getPattern());
        Assert.assertEquals("SHOW DATABASES LIKE 'abc'", stmt.toString());
        Assert.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());
    }
}