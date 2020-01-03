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

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexDefTest {
    private IndexDef def;

    @Before
    public void setUp() throws Exception {
        def = new IndexDef("index1", Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP, "balabala");
    }

    @Test
    public void testAnalyzeNormal() throws AnalysisException {
        def.analyze();
    }

    @Test
    public void testAnalyzeExpection() throws AnalysisException {
        try {
            def = new IndexDef(
                    "index1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxx"
                            + "xxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxinde"
                            + "x1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxx"
                            + "xxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxx"
                            + "xxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxx",
                    Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP,
                    "balabala");
            def.analyze();
            Assert.fail("No exception throws.");
        } catch (AnalysisException e) {
            Assert.assertTrue(e instanceof AnalysisException);
        }
        try {
            def = new IndexDef("", Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP, "balabala");
            def.analyze();
            Assert.fail("No exception throws.");
        } catch (AnalysisException e) {
            Assert.assertTrue(e instanceof AnalysisException);
        }
    }

    @Test
    public void toSql() {
        Assert.assertEquals("INDEX index1 (`col1`) USING BITMAP COMMENT 'balabala'", def.toSql());
        Assert.assertEquals("INDEX index1 ON table1 (`col1`) USING BITMAP COMMENT 'balabala'",
                def.toSql("table1"));
    }
}
