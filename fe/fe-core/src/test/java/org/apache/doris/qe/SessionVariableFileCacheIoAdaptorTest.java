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

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.thrift.TQueryOptions;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SessionVariableFileCacheIoAdaptorTest {
    private TQueryOptions readWireBoolean(short fieldId, boolean value) throws Exception {
        TMemoryBuffer buffer = new TMemoryBuffer(64);
        TBinaryProtocol protocol = new TBinaryProtocol(buffer);
        protocol.writeStructBegin(new TStruct("TQueryOptions"));
        protocol.writeFieldBegin(new TField("query_option", TType.BOOL, fieldId));
        protocol.writeBool(value);
        protocol.writeFieldEnd();
        protocol.writeFieldStop();
        protocol.writeStructEnd();
        TQueryOptions options = new TQueryOptions();
        options.read(protocol);
        return options;
    }

    @Test
    public void testUnrelatedFieldDoesNotEnableAdaptor() throws Exception {
        // A boolean from a newer frontend must not silently enable a different backend option.
        Assertions.assertFalse(readWireBoolean((short) 232, true).isSetEnableFileCacheIoAdaptor());
    }

    @Test
    public void testAdaptorWireFieldPreservesExplicitValues() throws Exception {
        for (boolean enabled : new boolean[] {true, false}) {
            TQueryOptions options = readWireBoolean((short) 1007, enabled);
            Assertions.assertTrue(options.isSetEnableFileCacheIoAdaptor());
            Assertions.assertEquals(enabled, options.isEnableFileCacheIoAdaptor());
        }
    }

    @Test
    public void testDefaultIsExplicitlySentToBackend() {
        TQueryOptions options = new SessionVariable().toThrift();
        Assertions.assertTrue(options.isSetEnableFileCacheIoAdaptor());
        Assertions.assertTrue(options.isEnableFileCacheIoAdaptor());
    }

    @Test
    public void testSessionOverrideAndForwardingAreIsolated() throws Exception {
        SessionVariable disabled = new SessionVariable();
        VariableMgr.setVar(disabled, new SetVar(SetType.SESSION,
                SessionVariable.ENABLE_FILE_CACHE_IO_ADAPTOR, new StringLiteral("false")));
        TQueryOptions options = disabled.toThrift();
        Assertions.assertTrue(options.isSetEnableFileCacheIoAdaptor());
        Assertions.assertFalse(options.isEnableFileCacheIoAdaptor());
        Assertions.assertEquals("false", disabled.getForwardVariables()
                .get(SessionVariable.ENABLE_FILE_CACHE_IO_ADAPTOR));
        SessionVariable forwarded = new SessionVariable();
        forwarded.setForwardedSessionVariables(disabled.getForwardVariables());
        Assertions.assertFalse(forwarded.toThrift().isEnableFileCacheIoAdaptor());
        Assertions.assertTrue(new SessionVariable().toThrift().isEnableFileCacheIoAdaptor());
    }
}
