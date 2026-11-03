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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TPaimonVectorPayload;
import org.apache.doris.thrift.TPaimonVectorPayloadType;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class PaimonVectorPayloadTest {
    @Test
    public void testVectorPayloadCoexistsWithReservedBlobFields() throws Exception {
        for (TProtocolFactory factory : Arrays.asList(new TBinaryProtocol.Factory(), new TCompactProtocol.Factory())) {
            TMemoryBuffer buffer = new TMemoryBuffer(256);
            TProtocol protocol = factory.getProtocol(buffer);
            TPaimonVectorPayload payload = new TPaimonVectorPayload()
                    .setPayloadType(TPaimonVectorPayloadType.BUCKET_SPLIT_BYTES)
                    .setBytes(ByteBuffer.wrap("PKVSPLIT".getBytes(StandardCharsets.UTF_8)));
            protocol.writeStructBegin(new TStruct("TPaimonFileDesc"));
            // branch-4.1 uses 21-23 for blob files and their anchor coordinates.
            protocol.writeFieldBegin(new TField("blob_files", TType.LIST, (short) 21));
            protocol.writeListBegin(new TList(TType.STRUCT, 0));
            protocol.writeListEnd();
            protocol.writeFieldEnd();
            protocol.writeFieldBegin(new TField("anchor_first_row_id", TType.I64, (short) 22));
            protocol.writeI64(100);
            protocol.writeFieldEnd();
            protocol.writeFieldBegin(new TField("anchor_row_count", TType.I64, (short) 23));
            protocol.writeI64(20);
            protocol.writeFieldEnd();
            protocol.writeFieldBegin(new TField("vector_payload", TType.STRUCT, (short) 24));
            payload.write(protocol);
            protocol.writeFieldEnd();
            protocol.writeFieldStop();
            protocol.writeStructEnd();

            TPaimonFileDesc decoded = new TPaimonFileDesc();
            decoded.read(protocol);
            Assertions.assertEquals(payload, decoded.getVectorPayload());
            TPaimonFileDesc roundTrip = new TPaimonFileDesc();
            new TDeserializer(factory).deserialize(roundTrip, new TSerializer(factory).serialize(decoded));
            Assertions.assertEquals(decoded, roundTrip);
            Assertions.assertEquals(24, TPaimonFileDesc._Fields.VECTOR_PAYLOAD.getThriftFieldId());
        }
    }

    @Test
    public void testPayloadMembersAreOptionalOnTheWire() throws Exception {
        // Decode partial payloads so the BE's explicit validation, rather than generated
        // required-field deserialization, owns the protocol mismatch diagnostics.
        for (TPaimonVectorPayload payload : Arrays.asList(new TPaimonVectorPayload(),
                new TPaimonVectorPayload().setPayloadType(TPaimonVectorPayloadType.BUCKET_SPLIT_BYTES),
                new TPaimonVectorPayload().setBytes(ByteBuffer.wrap(new byte[] {1})))) {
            TPaimonVectorPayload decoded = new TPaimonVectorPayload();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(payload));
            Assertions.assertEquals(payload, decoded);
        }
    }
}
