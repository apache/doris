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

import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class LanceThriftContractTest {

    @Test
    public void testLanceDescriptorCompactProtocolRoundTrip() throws Exception {
        TLanceFileDesc lanceDesc = new TLanceFileDesc()
                .setDatasetUri("s3://warehouse/db/table.lance")
                .setFragmentIds(Arrays.asList(7L, 11L))
                .setVersion(42L);
        TTableFormatFileDesc source = new TTableFormatFileDesc()
                .setTableFormatType(TableFormatType.LANCE.value())
                .setLanceParams(lanceDesc);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TTableFormatFileDesc restored = new TTableFormatFileDesc();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        Assert.assertEquals(TFileFormatType.FORMAT_LANCE.getValue(), 19);
        Assert.assertEquals(TableFormatType.LANCE.value(), restored.getTableFormatType());
        Assert.assertTrue(restored.isSetLanceParams());
        Assert.assertEquals("s3://warehouse/db/table.lance", restored.getLanceParams().getDatasetUri());
        Assert.assertEquals(Arrays.asList(7L, 11L), restored.getLanceParams().getFragmentIds());
        Assert.assertEquals(42L, restored.getLanceParams().getVersion());
    }
}
