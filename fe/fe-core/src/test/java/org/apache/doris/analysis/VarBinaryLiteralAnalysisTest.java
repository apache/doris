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

import org.apache.doris.common.FormatOptions;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TVarBinaryLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VarBinaryLiteralAnalysisTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testToSqlFormat() throws Exception {
        VarBinaryLiteral lit = new VarBinaryLiteral(bytes("hello"));
        Assertions.assertEquals("X'68656C6C6F'", lit.toSql());
    }

    @Test
    public void testGetStringValueAndNestedWrapper() throws Exception {
        VarBinaryLiteral lit = new VarBinaryLiteral(bytes("abc"));
        Assertions.assertEquals("abc", lit.getStringValue());

        FormatOptions opts = FormatOptions.getDefault();
        Assertions.assertEquals("\"abc\"", lit.getStringValueInComplexTypeForQuery(opts));
        FormatOptions hive = FormatOptions.getForHive();
        Assertions.assertEquals("\"abc\"", lit.getStringValueInComplexTypeForQuery(hive));
    }

    @Test
    public void testToThrift() throws Exception {
        VarBinaryLiteral lit = new VarBinaryLiteral(new byte[] { 'a', 0x00, 'b' });
        TExprNode node = new TExprNode();
        lit.toThrift(node);
        Assertions.assertEquals(TExprNodeType.VARBINARY_LITERAL, node.node_type);
        TVarBinaryLiteral v = node.getVarbinaryLiteral();
        Assertions.assertNotNull(v);
        ByteBuffer bb = v.bufferForValue();
        byte[] got = new byte[bb.remaining()];
        bb.get(got);
        Assertions.assertArrayEquals(new byte[] { 'a', 0x00, 'b' }, got);
    }

    @Test
    public void testCompareLiteralRules() throws Exception {
        VarBinaryLiteral ab = new VarBinaryLiteral(bytes("ab"));
        VarBinaryLiteral ab0 = new VarBinaryLiteral(new byte[] { 'a', 'b', 0x00 });
        VarBinaryLiteral ac = new VarBinaryLiteral(bytes("ac"));

        Assertions.assertEquals(0, ab.compareLiteral(ab));
        Assertions.assertEquals(0, ab.compareLiteral(ab0)); // trailing zero equals
        Assertions.assertTrue(ab.compareLiteral(ac) < 0);
        Assertions.assertTrue(ac.compareLiteral(ab) > 0);

        // null literal ordering
        Assertions.assertTrue(ab.compareLiteral(new NullLiteral()) > 0);
    }

    @Test
    public void testClone() throws Exception {
        VarBinaryLiteral a = new VarBinaryLiteral(bytes("xy"));
        VarBinaryLiteral b = (VarBinaryLiteral) a.clone();
        Assertions.assertNotSame(a, b);
        Assertions.assertEquals(a.getStringValue(), b.getStringValue());
        TExprNode node = new TExprNode();
        b.toThrift(node);
        Assertions.assertEquals(TExprNodeType.VARBINARY_LITERAL, node.node_type);
    }
}
