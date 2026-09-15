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
import org.apache.doris.thrift.TLanceIndexCompletionReason;
import org.apache.doris.thrift.TLanceIndexJobDispatch;
import org.apache.doris.thrift.TLanceIndexJobReport;
import org.apache.doris.thrift.TLanceIndexJobResultCode;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TLanceIndexTerminationProof;
import org.apache.doris.thrift.TLanceScanParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.meta_data.FieldMetaData;
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

    @Test
    public void testLanceIndexJobDispatchCompactRoundTrip() throws Exception {
        TLanceIndexJobDispatch source = new TLanceIndexJobDispatch()
                .setJobId(11L)
                .setDispatchRevision(3L)
                .setInvocationId("0f1e2d3c-dispatch")
                .setBeProcessEpoch(55L)
                .setDeadlineMs(1760000000000L)
                .setMutationType(TLanceIndexMutationType.REPLACE)
                .setIndexName("OrdersIdx")
                .setColumnName("vec")
                .setIndexType("IVF_PQ")
                .setPropertiesJson("{\"num_partitions\":4096}")
                .setIfNotExists(false)
                .setIfExists(true)
                .setDatasetUri("s3://warehouse/db/table.lance")
                .setAdmittedDatasetVersion(42L)
                .setSchemaContractJson("{\"version\":1}")
                .setStorageOptions(lanceStorageOptions());

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TLanceIndexJobDispatch restored = new TLanceIndexJobDispatch();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        Assert.assertEquals(11L, restored.getJobId());
        Assert.assertEquals(3L, restored.getDispatchRevision());
        Assert.assertEquals("0f1e2d3c-dispatch", restored.getInvocationId());
        Assert.assertEquals(55L, restored.getBeProcessEpoch());
        Assert.assertEquals(1760000000000L, restored.getDeadlineMs());
        Assert.assertEquals(TLanceIndexMutationType.REPLACE, restored.getMutationType());
        Assert.assertEquals("OrdersIdx", restored.getIndexName());
        Assert.assertEquals("vec", restored.getColumnName());
        Assert.assertEquals("IVF_PQ", restored.getIndexType());
        Assert.assertEquals("{\"num_partitions\":4096}", restored.getPropertiesJson());
        // Optional booleans must distinguish both states, not just "true".
        Assert.assertTrue(restored.isSetIfNotExists());
        Assert.assertFalse(restored.isIfNotExists());
        Assert.assertTrue(restored.isSetIfExists());
        Assert.assertTrue(restored.isIfExists());
        Assert.assertEquals("s3://warehouse/db/table.lance", restored.getDatasetUri());
        Assert.assertEquals(42L, restored.getAdmittedDatasetVersion());
        Assert.assertEquals("{\"version\":1}", restored.getSchemaContractJson());
        Assert.assertTrue(restored.isSetStorageOptions());
        Assert.assertEquals(lanceStorageOptions(), restored.getStorageOptions());
    }

    @Test
    public void testLanceIndexJobDispatchLeavesOptionalFieldsUnset() throws Exception {
        TLanceIndexJobDispatch source = new TLanceIndexJobDispatch()
                .setJobId(1L)
                .setDispatchRevision(1L)
                .setInvocationId("inv")
                .setBeProcessEpoch(1L)
                .setDeadlineMs(1L)
                .setMutationType(TLanceIndexMutationType.CREATE)
                .setIndexName("idx")
                .setColumnName("v")
                .setIndexType("IVF_PQ")
                .setDatasetUri("file:///data/ds")
                .setAdmittedDatasetVersion(1L)
                .setSchemaContractJson("{}");

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TLanceIndexJobDispatch restored = new TLanceIndexJobDispatch();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        // A dispatch without properties, IF flags, or credentials must round-trip with those
        // fields unset: the worker treats each absence as its own meaning, and a local
        // dataset carries no storage options at all.
        Assert.assertFalse(restored.isSetPropertiesJson());
        Assert.assertFalse(restored.isSetIfNotExists());
        Assert.assertFalse(restored.isSetIfExists());
        Assert.assertFalse(restored.isSetStorageOptions());
        Assert.assertEquals(TLanceIndexMutationType.CREATE, restored.getMutationType());
        Assert.assertEquals("file:///data/ds", restored.getDatasetUri());
    }

    @Test
    public void testLanceIndexJobReportCompactRoundTrip() throws Exception {
        TLanceIndexJobReport source = new TLanceIndexJobReport()
                .setJobId(9L)
                .setDispatchRevision(4L)
                .setInvocationId("0f1e2d3c-report")
                .setBeProcessEpoch(66L)
                .setResultCode(TLanceIndexJobResultCode.NATIVE_NOT_FOUND)
                .setCompletionReason(TLanceIndexCompletionReason.IF_CONDITION_NOOP)
                .setSanitizedMessage("index absent on the provider")
                .setExternalMetadataAdvanced(true)
                .setTerminationProof(TLanceIndexTerminationProof.CHILD_REAPED);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TLanceIndexJobReport restored = new TLanceIndexJobReport();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        Assert.assertEquals(9L, restored.getJobId());
        Assert.assertEquals(4L, restored.getDispatchRevision());
        Assert.assertEquals("0f1e2d3c-report", restored.getInvocationId());
        Assert.assertEquals(66L, restored.getBeProcessEpoch());
        Assert.assertEquals(TLanceIndexJobResultCode.NATIVE_NOT_FOUND, restored.getResultCode());
        Assert.assertEquals(TLanceIndexCompletionReason.IF_CONDITION_NOOP, restored.getCompletionReason());
        Assert.assertEquals("index absent on the provider", restored.getSanitizedMessage());
        Assert.assertTrue(restored.isSetExternalMetadataAdvanced());
        Assert.assertTrue(restored.isExternalMetadataAdvanced());
        Assert.assertEquals(TLanceIndexTerminationProof.CHILD_REAPED, restored.getTerminationProof());
    }

    @Test
    public void testLanceIndexJobReportLeavesOptionalFieldsUnset() throws Exception {
        TLanceIndexJobReport source = new TLanceIndexJobReport()
                .setJobId(9L)
                .setDispatchRevision(4L)
                .setInvocationId("inv")
                .setBeProcessEpoch(66L)
                .setResultCode(TLanceIndexJobResultCode.NATIVE_OK);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TLanceIndexJobReport restored = new TLanceIndexJobReport();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        // The minimal honest result: a code and nothing else. Each absent optional field is
        // its own meaning (NONE reason, no message, no advancement, no proof).
        Assert.assertFalse(restored.isSetCompletionReason());
        Assert.assertFalse(restored.isSetSanitizedMessage());
        Assert.assertFalse(restored.isSetExternalMetadataAdvanced());
        Assert.assertFalse(restored.isSetTerminationProof());
        Assert.assertEquals(TLanceIndexJobResultCode.NATIVE_OK, restored.getResultCode());
    }

    @Test
    public void testLanceIndexJobEnumNumberingMatchesTheIdl() {
        // Explicit wire numbers are a permanent contract for the worker side; drift here is a
        // protocol break, not a rename.
        Assert.assertEquals(1, TLanceIndexMutationType.CREATE.getValue());
        Assert.assertEquals(2, TLanceIndexMutationType.REPLACE.getValue());
        Assert.assertEquals(3, TLanceIndexMutationType.DROP.getValue());

        Assert.assertEquals(1, TLanceIndexJobResultCode.PRE_INVOCATION_STALE_ADMISSION.getValue());
        Assert.assertEquals(2, TLanceIndexJobResultCode.PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT.getValue());
        Assert.assertEquals(3, TLanceIndexJobResultCode.PRE_INVOCATION_CREDENTIAL_EXPIRED.getValue());
        Assert.assertEquals(4, TLanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED.getValue());
        Assert.assertEquals(5, TLanceIndexJobResultCode.NATIVE_OK.getValue());
        Assert.assertEquals(6, TLanceIndexJobResultCode.NATIVE_COMMIT_CONFLICT.getValue());
        Assert.assertEquals(7, TLanceIndexJobResultCode.NATIVE_NOT_FOUND.getValue());
        Assert.assertEquals(8, TLanceIndexJobResultCode.NATIVE_INVALID_ARGUMENT.getValue());
        Assert.assertEquals(9, TLanceIndexJobResultCode.NATIVE_NOT_SUPPORTED.getValue());
        Assert.assertEquals(10, TLanceIndexJobResultCode.NATIVE_INDEX.getValue());
        Assert.assertEquals(11, TLanceIndexJobResultCode.NATIVE_IO.getValue());
        Assert.assertEquals(12, TLanceIndexJobResultCode.NATIVE_INTERNAL.getValue());

        Assert.assertEquals(1, TLanceIndexCompletionReason.NONE.getValue());
        Assert.assertEquals(2, TLanceIndexCompletionReason.IF_CONDITION_NOOP.getValue());

        Assert.assertEquals(1, TLanceIndexTerminationProof.NONE.getValue());
        Assert.assertEquals(2, TLanceIndexTerminationProof.CHILD_REAPED.getValue());

        // NO_TRUSTED_RESULT is FE-side only and must never gain a wire number.
        Assert.assertEquals(12, TLanceIndexJobResultCode.values().length);
        for (TLanceIndexJobResultCode code : TLanceIndexJobResultCode.values()) {
            Assert.assertNotEquals("NO_TRUSTED_RESULT", code.name());
        }
    }

    @Test
    public void testLanceIndexJobEnumFindByValueResolvesEveryConstant() {
        for (TLanceIndexMutationType value : TLanceIndexMutationType.values()) {
            Assert.assertSame(value, TLanceIndexMutationType.findByValue(value.getValue()));
        }
        for (TLanceIndexJobResultCode value : TLanceIndexJobResultCode.values()) {
            Assert.assertSame(value, TLanceIndexJobResultCode.findByValue(value.getValue()));
        }
        for (TLanceIndexCompletionReason value : TLanceIndexCompletionReason.values()) {
            Assert.assertSame(value, TLanceIndexCompletionReason.findByValue(value.getValue()));
        }
        for (TLanceIndexTerminationProof value : TLanceIndexTerminationProof.values()) {
            Assert.assertSame(value, TLanceIndexTerminationProof.findByValue(value.getValue()));
        }
        // Unknown numbers (including 0, the thrift default) resolve to null, never to a
        // wrong constant; the report handler drops such envelopes.
        Assert.assertNull(TLanceIndexMutationType.findByValue(0));
        Assert.assertNull(TLanceIndexMutationType.findByValue(4));
        Assert.assertNull(TLanceIndexJobResultCode.findByValue(0));
        Assert.assertNull(TLanceIndexJobResultCode.findByValue(13));
        Assert.assertNull(TLanceIndexCompletionReason.findByValue(0));
        Assert.assertNull(TLanceIndexCompletionReason.findByValue(3));
        Assert.assertNull(TLanceIndexTerminationProof.findByValue(0));
        Assert.assertNull(TLanceIndexTerminationProof.findByValue(3));
    }

    @Test
    public void testLanceIndexJobStructFieldIdsMatchTheIdl() {
        // Explicit field ids are the wire-drift defense of the dedicated channel. A
        // symmetric round-trip cannot catch renumbering (reader and writer move
        // together), so the ids are pinned against the generated metadata directly.
        Map<String, Integer> expectedDispatchIds = new HashMap<>();
        expectedDispatchIds.put("job_id", 1);
        expectedDispatchIds.put("dispatch_revision", 2);
        expectedDispatchIds.put("invocation_id", 3);
        expectedDispatchIds.put("be_process_epoch", 4);
        expectedDispatchIds.put("deadline_ms", 5);
        expectedDispatchIds.put("mutation_type", 6);
        expectedDispatchIds.put("index_name", 7);
        expectedDispatchIds.put("column_name", 8);
        expectedDispatchIds.put("index_type", 9);
        expectedDispatchIds.put("properties_json", 10);
        expectedDispatchIds.put("if_not_exists", 11);
        expectedDispatchIds.put("if_exists", 12);
        expectedDispatchIds.put("dataset_uri", 13);
        expectedDispatchIds.put("admitted_dataset_version", 14);
        expectedDispatchIds.put("schema_contract_json", 15);
        expectedDispatchIds.put("storage_options", 16);
        Assert.assertEquals(expectedDispatchIds, fieldIdsByName(TLanceIndexJobDispatch.metaDataMap));

        Map<String, Integer> expectedReportIds = new HashMap<>();
        expectedReportIds.put("job_id", 1);
        expectedReportIds.put("dispatch_revision", 2);
        expectedReportIds.put("invocation_id", 3);
        expectedReportIds.put("be_process_epoch", 4);
        expectedReportIds.put("result_code", 5);
        expectedReportIds.put("completion_reason", 6);
        expectedReportIds.put("sanitized_message", 7);
        expectedReportIds.put("external_metadata_advanced", 8);
        expectedReportIds.put("termination_proof", 9);
        Assert.assertEquals(expectedReportIds, fieldIdsByName(TLanceIndexJobReport.metaDataMap));
    }

    private static Map<String, Integer> fieldIdsByName(
            Map<? extends TFieldIdEnum, FieldMetaData> metaDataMap) {
        Map<String, Integer> ids = new HashMap<>();
        for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> entry : metaDataMap.entrySet()) {
            ids.put(entry.getValue().fieldName, (int) entry.getKey().getThriftFieldId());
        }
        return ids;
    }

    @Test
    public void testDispatchStorageOptionsReachTheWireUntouched() throws Exception {
        // Whatever the namespace vends has to reach the worker untranslated inside the
        // dispatch too, including values Doris itself assigns no meaning to (empty strings).
        Map<String, String> options = new HashMap<>();
        options.put("access_key_id", "ak");
        options.put("endpoint", "http://127.0.0.1:9000");
        options.put("azure_storage_sas_token", "");
        TLanceIndexJobDispatch source = minimalDispatch().setStorageOptions(options);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TLanceIndexJobDispatch restored = new TLanceIndexJobDispatch();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        Assert.assertEquals(options, restored.getStorageOptions());
        Assert.assertEquals("", restored.getStorageOptions().get("azure_storage_sas_token"));
    }

    private static Map<String, String> lanceStorageOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("access_key_id", "ak");
        options.put("secret_access_key", "sk");
        options.put("endpoint", "http://127.0.0.1:9000");
        options.put("expires_at_millis", "1760000000000");
        return options;
    }

    private static TLanceIndexJobDispatch minimalDispatch() {
        return new TLanceIndexJobDispatch()
                .setJobId(1L)
                .setDispatchRevision(1L)
                .setInvocationId("inv")
                .setBeProcessEpoch(1L)
                .setDeadlineMs(1L)
                .setMutationType(TLanceIndexMutationType.CREATE)
                .setIndexName("idx")
                .setColumnName("v")
                .setIndexType("IVF_PQ")
                .setDatasetUri("s3://bucket/dataset")
                .setAdmittedDatasetVersion(1L)
                .setSchemaContractJson("{}");
    }
}
