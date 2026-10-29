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

package org.apache.doris.datasource;

import org.apache.doris.thrift.THiveTableSink;

import com.google.common.collect.ImmutableList;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class ExternalTimestampWireContractTest {
    @Test
    public void hiveTimezoneDoesNotReuseMasterAzureField() throws Exception {
        for (TProtocolFactory factory : ImmutableList.of(new TBinaryProtocol.Factory(), new TCompactProtocol.Factory())) {
            for (String zone : Arrays.asList(null, "", "UTC", "Asia/Shanghai")) {
                TMemoryBuffer transport = new TMemoryBuffer(128);
                TProtocol protocol = factory.getProtocol(transport);
                protocol.writeStructBegin(new TStruct("THiveTableSink"));
                // Field 13 belongs to the maintained master's Azure multipart capability.
                protocol.writeFieldBegin(new TField("supports_deferred_azure_multipart", TType.BOOL, (short) 13));
                protocol.writeBool(true);
                protocol.writeFieldEnd();
                if (zone != null) {
                    protocol.writeFieldBegin(new TField("hive_parquet_time_zone", TType.STRING, (short) 14));
                    protocol.writeString(zone);
                    protocol.writeFieldEnd();
                }
                protocol.writeFieldStop();
                protocol.writeStructEnd();
                THiveTableSink sink = new THiveTableSink();
                sink.read(factory.getProtocol(transport));
                Assertions.assertEquals(zone != null, sink.isSetHiveParquetTimeZone());
                Assertions.assertEquals(zone, sink.getHiveParquetTimeZone());

                TMemoryBuffer output = new TMemoryBuffer(128);
                sink.write(factory.getProtocol(output));
                TProtocol reader = factory.getProtocol(output);
                reader.readStructBegin();
                boolean found = false;
                for (TField field = reader.readFieldBegin(); field.type != TType.STOP;
                        field = reader.readFieldBegin()) {
                    Assertions.assertNotEquals(13, field.id);
                    if (field.id == 14) {
                        Assertions.assertEquals(TType.STRING, field.type);
                        Assertions.assertEquals(zone, reader.readString());
                        found = true;
                    } else {
                        TProtocolUtil.skip(reader, field.type);
                    }
                    reader.readFieldEnd();
                }
                reader.readStructEnd();
                Assertions.assertEquals(zone != null, found);
            }
        }
    }
}
