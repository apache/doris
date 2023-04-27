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

import org.apache.doris.qe.ShowResultSetMetaData;

import org.junit.Assert;
import org.junit.Test;

public class ShowEnginesStmtTest {
    @Test
    public void testNormal() {
        ShowEnginesStmt stmt = new ShowEnginesStmt();
        stmt.analyze(null);
        Assert.assertEquals("SHOW ENGINES", stmt.toString());
        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assert.assertNotNull(metaData);
        Assert.assertEquals(6, metaData.getColumnCount());
        Assert.assertEquals("Engine", metaData.getColumn(0).getName());
        Assert.assertEquals("Support", metaData.getColumn(1).getName());
        Assert.assertEquals("Comment", metaData.getColumn(2).getName());
        Assert.assertEquals("Transactions", metaData.getColumn(3).getName());
        Assert.assertEquals("XA", metaData.getColumn(4).getName());
        Assert.assertEquals("Savepoints", metaData.getColumn(5).getName());
    }

}
