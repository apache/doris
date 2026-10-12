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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TUUIDLiteral;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

class UuidLiteralTest {

    @Test
    void parseCanonicalAndCompactForms() {
        UuidLiteral canonical = new UuidLiteral("550E8400-E29B-41D4-A716-446655440000");
        UuidLiteral compact = new UuidLiteral("550e8400e29b41d4a716446655440000");

        Assertions.assertEquals(UuidType.INSTANCE, canonical.getDataType());
        Assertions.assertEquals("550e8400-e29b-41d4-a716-446655440000", canonical.getValue().toString());
        Assertions.assertEquals(canonical.getValue(), compact.getValue());
        Assertions.assertEquals("550e8400-e29b-41d4-a716-446655440000",
                canonical.toLegacyLiteral().getStringValue());
    }

    @Test
    void rejectInvalidForms() {
        Assertions.assertThrows(AnalysisException.class,
                () -> new UuidLiteral("550e8400-e29b-41d4-a716-44665544000g"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new UuidLiteral("{550e8400-e29b-41d4-a716-446655440000}"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new UuidLiteral("550e8400e29b-41d4-a716-446655440000"));
    }

    @Test
    void compareAsUnsigned128BitValues() {
        UuidLiteral smaller = new UuidLiteral("00000000-0000-0000-ffff-ffffffffffff");
        UuidLiteral larger = new UuidLiteral("00000000-0000-0001-0000-000000000000");
        UuidLiteral highBit = new UuidLiteral("80000000-0000-0000-0000-000000000000");

        Assertions.assertTrue(smaller.compareTo(larger) < 0);
        Assertions.assertTrue(larger.compareTo(highBit) < 0);
    }

    @Test
    void thriftPreservesAll128Bits() throws Exception {
        Method deserialize = ConnectProcessor.class.getDeclaredMethod("getLiteralExprFromThrift", TExprNode.class);
        deserialize.setAccessible(true);
        List<TProtocolFactory> protocols = List.of(new TBinaryProtocol.Factory(), new TCompactProtocol.Factory());
        for (String text : List.of("00000000-0000-0000-0000-000000000000",
                "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-8000-000000000000",
                "00000000-0000-0000-ffff-ffffffffffff", "00000000-0000-0001-0000-000000000000",
                "7fffffff-ffff-ffff-ffff-ffffffffffff", "80000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff", "00112233445566778899AABBCCDDEEFF")) {
            UuidLiteral literal = new UuidLiteral(text);
            UUID value = literal.getValue();
            TExprNode node = ExprToThriftVisitor.treeToThrift(literal.toLegacyLiteral()).getNodes().get(0);
            Assertions.assertEquals(value.getMostSignificantBits(), node.uuid_literal.hi);
            Assertions.assertEquals(value.getLeastSignificantBits(), node.uuid_literal.lo);
            byte[] expected = ByteBuffer.allocate(23)
                    .put(TType.I64).putShort((short) 1).putLong(value.getMostSignificantBits())
                    .put(TType.I64).putShort((short) 2).putLong(value.getLeastSignificantBits())
                    .put(TType.STOP).array();
            Assertions.assertArrayEquals(expected, new TSerializer().serialize(node.uuid_literal));
            for (TProtocolFactory protocol : protocols) {
                byte[] bytes = new TSerializer(protocol).serialize(node);
                TExprNode restoredNode = new TExprNode();
                new TDeserializer(protocol).deserialize(restoredNode, bytes);
                LiteralExpr restored = (LiteralExpr) deserialize.invoke(null, restoredNode);
                Assertions.assertEquals(Type.UUID, restored.getType());
                Assertions.assertEquals(value.toString(), restored.getStringValue());
            }
        }
    }

    @Test
    void thriftRequiresBothHalves() {
        for (short fieldId : new short[] {1, 2}) {
            byte[] bytes = ByteBuffer.allocate(12).put(TType.I64).putShort(fieldId).putLong(0).put(TType.STOP).array();
            Assertions.assertThrows(TException.class,
                    () -> new TDeserializer().deserialize(new TUUIDLiteral(), bytes));
        }
    }
}
