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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import org.junit.Assert;
import org.junit.Test;

public class VariableExprTest {

    @Test
    public void testNormal() throws AnalysisException {
        VariableExpr desc = new VariableExpr("version_comment");
        desc.analyze(AccessTestUtil.fetchAdminAnalyzer(false));
        Assert.assertEquals("@@version_comment", desc.toSql());
        Assert.assertEquals("version_comment", desc.getName());
        Assert.assertEquals(ScalarType.createType(PrimitiveType.VARCHAR), desc.getType());
        Assert.assertEquals(SetType.SESSION, desc.getSetType());

        TExprNode tNode = new TExprNode();
        desc.toThrift(tNode);
        Assert.assertEquals(tNode.node_type, TExprNodeType.STRING_LITERAL);
        Assert.assertNotNull(tNode.string_literal);
    }

    @Test(expected = AnalysisException.class)
    public void testNoVar() throws AnalysisException {
        VariableExpr desc = new VariableExpr("zcPrivate");
        desc.analyze(AccessTestUtil.fetchAdminAnalyzer(false));
        Assert.fail("No exception throws.");
    }

}
