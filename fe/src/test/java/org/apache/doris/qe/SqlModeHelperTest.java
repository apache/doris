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
        Assert.assertEquals(true, SqlModeHelper.checkValid(sqlMode));
        Assert.assertEquals(new Long(2L), SqlModeHelper.parseString(sqlMode));

        sqlMode = "PIPES_AS_CONCAT, WRONG_MODE";
        Assert.assertEquals(false, SqlModeHelper.checkValid(sqlMode));
        Assert.assertEquals(new Long(0L), SqlModeHelper.parseString(sqlMode));

        sqlMode = "";
        Assert.assertEquals(false, SqlModeHelper.parseString(sqlMode));
        Assert.assertEquals(new Long(0L), SqlModeHelper.parseString(sqlMode));

        long sqlModeValue = 2L;
        Assert.assertEquals(true, SqlModeHelper.checkValid(sqlModeValue));
        Assert.assertEquals("PIPES_AS_CONCAT", SqlModeHelper.parseValue(2L));

        sqlModeValue = Long.MAX_VALUE;
        Assert.assertEquals(false, SqlModeHelper.checkValid(sqlModeValue));
        Assert.assertEquals("", SqlModeHelper.parseValue(sqlModeValue));

        sqlModeValue = 0L;
        Assert.assertEquals(false, SqlModeHelper.checkValid(sqlModeValue));
        Assert.assertEquals("", SqlModeHelper.parseValue(sqlModeValue));
    }
}
