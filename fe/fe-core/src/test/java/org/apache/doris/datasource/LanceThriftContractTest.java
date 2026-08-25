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
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class LanceThriftContractTest {

    @Test
    public void testLanceDescriptorCompactProtocolRoundTrip() throws Exception {
        TLanceFileDesc lanceDesc = new TLanceFileDesc()
                .setDatasetUri("s3://warehouse/db/table.lance")
                .setFragmentIds(Arrays.asList(7L, 11L))
                .setVersion(42L)
                .setLimit(100L);
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
        Assert.assertTrue(restored.getLanceParams().isSetLimit());
        Assert.assertEquals(100L, restored.getLanceParams().getLimit());
    }

    @Test
    public void testLanceDescriptorWithoutLimit() throws Exception {
        TLanceFileDesc lanceDesc = new TLanceFileDesc()
                .setDatasetUri("s3://warehouse/db/table.lance")
                .setFragmentIds(Arrays.asList(1L))
                .setVersion(1L);
        TTableFormatFileDesc source = new TTableFormatFileDesc()
                .setTableFormatType(TableFormatType.LANCE.value())
                .setLanceParams(lanceDesc);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TTableFormatFileDesc restored = new TTableFormatFileDesc();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        // A scan without a pushable LIMIT must leave the field unset so the BE reads all rows.
        Assert.assertFalse(restored.getLanceParams().isSetLimit());
    }

    @Test
    public void testLanceStorageOptionsSurviveRoundTripUntouched() throws Exception {
        Map<String, String> storageOptions = new HashMap<>();
        storageOptions.put("access_key_id", "ak");
        storageOptions.put("secret_access_key", "sk");
        storageOptions.put("endpoint", "http://127.0.0.1:9000");
        storageOptions.put("expires_at_millis", "1760000000000");
        storageOptions.put("azure_storage_sas_token", "sas");

        TFileScanRangeParams source = new TFileScanRangeParams()
                .setFormatType(TFileFormatType.FORMAT_LANCE)
                .setLanceStorageOptions(storageOptions);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TFileScanRangeParams restored = new TFileScanRangeParams();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        // Whatever the namespace vended has to reach lance-c unchanged, including keys Doris
        // itself assigns no meaning to.
        Assert.assertTrue(restored.isSetLanceStorageOptions());
        Assert.assertEquals(storageOptions, restored.getLanceStorageOptions());
    }

    @Test
    public void testLanceStorageOptionsAreOptional() throws Exception {
        TFileScanRangeParams source = new TFileScanRangeParams()
                .setFormatType(TFileFormatType.FORMAT_LANCE);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TFileScanRangeParams restored = new TFileScanRangeParams();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        // A local dataset needs no storage configuration at all.
        Assert.assertFalse(restored.isSetLanceStorageOptions());
    }
}
