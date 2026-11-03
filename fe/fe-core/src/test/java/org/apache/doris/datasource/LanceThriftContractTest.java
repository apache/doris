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

import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TLanceIndexMutationRequest;
import org.apache.doris.thrift.TLanceIndexMutationResult;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TLanceScanParams;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LanceThriftContractTest {

    @Test
    public void testScalarIndexTaskCompactProtocolRoundTrip() throws Exception {
        for (boolean indexed : new boolean[] {true, false}) {
            TLanceFileDesc source = new TLanceFileDesc()
                    .setDatasetUri("s3://warehouse/db/table.lance")
                    .setVersion(42L).setFragmentIds(Arrays.asList(7L, 11L));
            if (indexed) {
                ByteBuffer segmentUuid = ByteBuffer.allocate(16).putLong(1).putLong(2);
                segmentUuid.flip();
                source.setIndexSegmentUuids(Collections.singletonList(segmentUuid));
            } else {
                source.setUseScalarIndex(false);
            }
            TLanceFileDesc restored = new TLanceFileDesc();
            new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored,
                    new TSerializer(new TCompactProtocol.Factory()).serialize(source));
            Assert.assertEquals(source, restored);
            Assert.assertEquals(!indexed, restored.isSetUseScalarIndex());
            Assert.assertEquals(indexed, restored.isSetIndexSegmentUuids());
        }
    }

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
                .setLanceScanParams(
                        new TLanceScanParams().setLanceStorageOptions(storageOptions));

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TFileScanRangeParams restored = new TFileScanRangeParams();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        // Whatever the namespace vended has to reach lance-c unchanged, including keys Doris
        // itself assigns no meaning to.
        Assert.assertTrue(restored.isSetLanceScanParams());
        Assert.assertTrue(restored.getLanceScanParams().isSetLanceStorageOptions());
        Assert.assertEquals(storageOptions,
                restored.getLanceScanParams().getLanceStorageOptions());
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
        Assert.assertFalse(restored.isSetLanceScanParams());
    }

    /**
     * Pins the wire contract of the synchronous Lance index mutation dispatch: field ids
     * and IDL snake_case names must never drift, because a struct field id consumed by a
     * backend cannot be renumbered once released. Extending a struct means appending a new
     * id and updating this pin consciously.
     */
    private static void assertContractField(String structName, TFieldIdEnum field, short expectedId,
            String expectedName, Map<? extends TFieldIdEnum, FieldMetaData> metaDataMap) {
        Assert.assertNotNull(structName + "." + expectedName + " must stay in the metadata map",
                metaDataMap.get(field));
        Assert.assertEquals(structName + " field id of " + expectedName,
                expectedId, field.getThriftFieldId());
        Assert.assertEquals(structName + " metadata name must stay the IDL snake_case name",
                expectedName, metaDataMap.get(field).fieldName);
    }

    @Test
    public void testLanceIndexMutationRequestContractIsPinned() {
        Map<TLanceIndexMutationRequest._Fields, FieldMetaData> metaDataMap =
                TLanceIndexMutationRequest.metaDataMap;
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.INVOCATION_ID, (short) 1, "invocation_id", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.BE_PROCESS_EPOCH, (short) 2, "be_process_epoch",
                metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.DEADLINE_MS, (short) 3, "deadline_ms", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.MUTATION_TYPE, (short) 4, "mutation_type", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.INDEX_NAME, (short) 5, "index_name", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.COLUMN_NAME, (short) 6, "column_name", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.INDEX_TYPE, (short) 7, "index_type", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.PROPERTIES_JSON, (short) 8, "properties_json",
                metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.IF_NOT_EXISTS, (short) 9, "if_not_exists", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.IF_EXISTS, (short) 10, "if_exists", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.DATASET_URI, (short) 11, "dataset_uri", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.ADMITTED_DATASET_VERSION, (short) 12,
                "admitted_dataset_version", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.SCHEMA_CONTRACT_JSON, (short) 13,
                "schema_contract_json", metaDataMap);
        assertContractField("TLanceIndexMutationRequest",
                TLanceIndexMutationRequest._Fields.STORAGE_OPTIONS, (short) 14, "storage_options",
                metaDataMap);
        // Adding a field is fine (append an id, extend this pin); silently renumbering or
        // reusing an id is not, so the struct width is pinned too.
        Assert.assertEquals(14, metaDataMap.size());
    }

    @Test
    public void testLanceIndexMutationResultContractIsPinned() {
        Map<TLanceIndexMutationResult._Fields, FieldMetaData> metaDataMap =
                TLanceIndexMutationResult.metaDataMap;
        assertContractField("TLanceIndexMutationResult",
                TLanceIndexMutationResult._Fields.STATUS, (short) 1, "status", metaDataMap);
        assertContractField("TLanceIndexMutationResult",
                TLanceIndexMutationResult._Fields.LANCE_ERROR_CODE, (short) 2, "lance_error_code",
                metaDataMap);
        assertContractField("TLanceIndexMutationResult",
                TLanceIndexMutationResult._Fields.MESSAGE, (short) 3, "message", metaDataMap);
        // Worker-phase markers of later slices must be appended at ids 4+.
        Assert.assertEquals(3, metaDataMap.size());
    }

    @Test
    public void testLanceIndexMutationTypeValuesArePinned() {
        Assert.assertEquals(1, TLanceIndexMutationType.CREATE.getValue());
        Assert.assertEquals(2, TLanceIndexMutationType.REPLACE.getValue());
        Assert.assertEquals(3, TLanceIndexMutationType.DROP.getValue());
        Assert.assertEquals(3, TLanceIndexMutationType.values().length);
    }

    @Test
    public void testBackendServiceServesLanceIndexMutate() throws Exception {
        // The dispatch RPC must exist on the backend service interface with the contract
        // request/result pair; fullcamel generation maps the IDL name lance_index_mutate.
        Method mutate = BackendService.Iface.class.getMethod("lanceIndexMutate",
                TLanceIndexMutationRequest.class);
        Assert.assertEquals(TLanceIndexMutationResult.class, mutate.getReturnType());
    }

    @Test
    public void testLanceIndexMutationCompactProtocolRoundTrip() throws Exception {
        Map<String, String> storageOptions = new HashMap<>();
        storageOptions.put("endpoint", "http://127.0.0.1:9000");
        storageOptions.put("expires_at_millis", "1760000000000");
        TLanceIndexMutationRequest request = new TLanceIndexMutationRequest()
                .setInvocationId("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
                .setBeProcessEpoch(7L)
                .setDeadlineMs(1760000000000L)
                .setMutationType(TLanceIndexMutationType.REPLACE)
                .setIndexName("embedding")
                .setColumnName("embedding")
                .setIndexType("IVF_PQ")
                .setPropertiesJson("{\"metric\":\"l2\"}")
                .setIfNotExists(false)
                .setIfExists(true)
                .setDatasetUri("s3://warehouse/db/table.lance")
                .setAdmittedDatasetVersion(42L)
                .setSchemaContractJson("{\"scv\":1,\"flds\":[]}")
                .setStorageOptions(storageOptions);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] requestBytes = serializer.serialize(request);
        TLanceIndexMutationRequest restoredRequest = new TLanceIndexMutationRequest();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restoredRequest, requestBytes);
        Assert.assertEquals(request, restoredRequest);
        // Provider-opaque storage options must survive the wire untouched.
        Assert.assertEquals(storageOptions, restoredRequest.getStorageOptions());

        TLanceIndexMutationResult result = new TLanceIndexMutationResult()
                .setStatus(new TStatus(TStatusCode.NOT_IMPLEMENTED_ERROR))
                .setMessage("lance index worker is not available in this build");
        byte[] resultBytes = serializer.serialize(result);
        TLanceIndexMutationResult restoredResult = new TLanceIndexMutationResult();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restoredResult, resultBytes);
        Assert.assertEquals(result, restoredResult);
        Assert.assertEquals(TStatusCode.NOT_IMPLEMENTED_ERROR, restoredResult.getStatus().getStatusCode());
        // The typed code is optional on the wire: a clean pre-invocation rejection carries
        // no native result, and the status alone is the complete classification input.
        Assert.assertFalse(restoredResult.isSetLanceErrorCode());
    }
}
