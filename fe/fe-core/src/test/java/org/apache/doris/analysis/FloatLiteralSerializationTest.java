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

import org.apache.doris.catalog.Type;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TFloatLiteral;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

public class FloatLiteralSerializationTest {
    @Test
    public void testBinaryProtocolPreservesNaNSign() throws Exception {
        checkNaNSign(new TBinaryProtocol.Factory());
    }

    @Test
    public void testCompactProtocolPreservesNaNSign() throws Exception {
        checkNaNSign(new TCompactProtocol.Factory());
    }

    @Test
    public void testVariablePreservesNaNSign() throws Exception {
        // User variables have a separate serialization entry point.
        // One negative NaN checks that this entry point uses the same encoding.
        VariableExpr variable = new VariableExpr("nan_value", SetType.USER);
        variable.setType(Type.DOUBLE);
        variable.setFloatValue(Double.longBitsToDouble(0xfff8000000000000L));
        TExpr restored = roundTrip(variable, new TBinaryProtocol.Factory());
        Assertions.assertEquals(0xfff8000000000000L, Double.doubleToRawLongBits(readValue(restored)));
    }

    @Test
    public void testLegacyMessagePreservesNegativeZero() throws Exception {
        // Old FE versions send only value. Negative zero checks this fallback path.
        TExpr source = ExprToThriftVisitor.treeToThrift(new FloatLiteral(-0.0, Type.DOUBLE));
        Assertions.assertFalse(source.nodes.get(0).float_literal.isSetRawBits());
        source.nodes.get(0).setFloatLiteral(new TFloatLiteral(-0.0));
        TExpr restored = new TExpr();
        new TDeserializer().deserialize(restored, new TSerializer().serialize(source));
        Assertions.assertEquals(0x8000000000000000L, Double.doubleToRawLongBits(readValue(restored)));
    }

    private void checkNaNSign(TProtocolFactory protocol) throws Exception {
        // A single negative NaN exposes the protocol error. The positive NaN is a control.
        // Serialize real bytes. An in-memory TExpr retains the sign and would hide the error.
        for (Type type : new Type[] {Type.FLOAT, Type.DOUBLE}) {
            for (long bits : new long[] {0xfff8000000000000L, 0x7ff8000000000000L}) {
                FloatLiteral literal = new FloatLiteral(Double.longBitsToDouble(bits), type);
                TExpr restored = roundTrip(literal, protocol);
                Assertions.assertEquals(bits, Double.doubleToRawLongBits(readValue(restored)));
            }
        }
    }

    private TExpr roundTrip(Expr expression, TProtocolFactory protocol) throws Exception {
        TExpr source = ExprToThriftVisitor.treeToThrift(expression);
        TExpr restored = new TExpr();
        new TDeserializer(protocol).deserialize(restored, new TSerializer(protocol).serialize(source));
        return restored;
    }

    private double readValue(TExpr expression) throws Exception {
        // FE forwarding reads the same literal fields as BE execution.
        // Check the returned value through the production FE reader.
        Method reader = ConnectProcessor.class.getDeclaredMethod("getLiteralExprFromThrift", TExprNode.class);
        reader.setAccessible(true);
        FloatLiteral result = (FloatLiteral) reader.invoke(null, expression.nodes.get(0));
        return result.getDoubleValue();
    }
}
