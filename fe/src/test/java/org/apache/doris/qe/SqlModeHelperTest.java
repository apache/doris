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

package org.apache.doris.qe;

import org.apache.doris.common.AnalysisException;
import org.junit.Assert;
import org.junit.Test;

public class SqlModeHelperTest {

    @Test
    public void testNormal() throws AnalysisException {
        String sqlMode = "PIPES_AS_CONCAT";
        Assert.assertEquals(new Long(2L), SqlModeHelper.encode(sqlMode));

        sqlMode = "";
        Assert.assertEquals(new Long(0L), SqlModeHelper.encode(sqlMode));

        long sqlModeValue = 2L;
        Assert.assertEquals("PIPES_AS_CONCAT", SqlModeHelper.decode(sqlModeValue));

        sqlModeValue = 0L;
        Assert.assertEquals("", SqlModeHelper.decode(sqlModeValue));
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidSqlMode() throws AnalysisException {
        String sqlMode = "PIPES_AS_CONCAT, WRONG_MODE";
        SqlModeHelper.encode(sqlMode);
        Assert.fail("No exception throws");
    }

    @Test(expected = AnalysisException.class)
    public void testMultiSqlMode() throws AnalysisException {
        String sqlMode = "ANSI, TRADITIONAL";
        SqlModeHelper.encode(sqlMode);
        Assert.fail("No exception throws");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidDecode() throws AnalysisException {
        long sqlMode = SqlModeHelper.MODE_LAST;
        SqlModeHelper.decode(sqlMode);
        Assert.fail("No exception throws");
    }
}
