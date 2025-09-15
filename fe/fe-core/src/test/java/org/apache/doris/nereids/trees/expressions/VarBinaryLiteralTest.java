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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.VarBinaryLiteral;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TVarBinaryLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VarBinaryLiteralTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testToSqlAndGetStringValue() throws Exception {
        VarBinaryLiteral lit = new VarBinaryLiteral(bytes("hello"));
        // toSql should output hex literal format X'HEX'
        Assertions.assertEquals("X'68656C6C6F'", lit.toSql());
        // getStringValue returns ISO_8859_1 decoded string (ascii here)
        Assertions.assertEquals("hello", lit.getStringValue());
        // also verify hex (via toSql without prefix/suffix extraction)
        Assertions.assertTrue(lit.toSql().contains("68656C6C6F"));

        // nested string wrapper behavior (still wraps the plain string value)
        FormatOptions opts = FormatOptions.getDefault();
        Assertions.assertEquals("\"hello\"", lit.getStringValueInComplexTypeForQuery(opts));

        // hive option uses same wrapper for nested by default
        FormatOptions hive = FormatOptions.getForHive();
        Assertions.assertEquals("\"hello\"", lit.getStringValueInComplexTypeForQuery(hive));
    }

    @Test
    public void testToThriftNode() throws Exception {
        VarBinaryLiteral lit = new VarBinaryLiteral(bytes("abc\0def"));
        TExprNode node = new TExprNode();
        lit.toThrift(node);
        Assertions.assertEquals(TExprNodeType.VARBINARY_LITERAL, node.node_type);
        TVarBinaryLiteral v = node.getVarbinaryLiteral();
        Assertions.assertNotNull(v);
        ByteBuffer bb = v.bufferForValue();
        // Verify content and length (including embedded null)
        byte[] got = new byte[bb.remaining()];
        bb.get(got);
        Assertions.assertArrayEquals(bytes("abc\0def"), got);
    }

    @Test
    public void testCompareLiteral() throws Exception {
        VarBinaryLiteral a = new VarBinaryLiteral(bytes("ab"));
        VarBinaryLiteral b = new VarBinaryLiteral(bytes("ab"));
        VarBinaryLiteral c = new VarBinaryLiteral(bytes("ac"));
        // equal
        Assertions.assertEquals(0, a.compareLiteral(b));
        // lexicographic compare by UTF-8 bytes
        Assertions.assertTrue(a.compareLiteral(c) < 0);
        Assertions.assertTrue(c.compareLiteral(a) > 0);

        // different length with trailing zero semantics
        VarBinaryLiteral withZero = new VarBinaryLiteral(new byte[] { 'a', 'b', 0x00 });
        Assertions.assertEquals(0, a.compareLiteral(withZero));

        // null literal is treated as less than any value
        Assertions.assertTrue(a.compareLiteral(new NullLiteral()) > 0);
    }

    @Test
    public void testCloneIndependence() throws Exception {
        VarBinaryLiteral a = new VarBinaryLiteral(bytes("xy"));
        VarBinaryLiteral b = (VarBinaryLiteral) a.clone();
        Assertions.assertNotSame(a, b);
        Assertions.assertEquals(a.getStringValue(), b.getStringValue());
        // thrift should be constructed correctly after clone
        TExprNode node = new TExprNode();
        b.toThrift(node);
        Assertions.assertEquals(TExprNodeType.VARBINARY_LITERAL, node.node_type);
    }
}
