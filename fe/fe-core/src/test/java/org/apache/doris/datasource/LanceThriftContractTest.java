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

import org.apache.doris.datasource.lance.job.LanceIndexDispatchBounds;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TLanceIndexCompletionReason;
import org.apache.doris.thrift.TLanceIndexJobDispatch;
import org.apache.doris.thrift.TLanceIndexJobReport;
import org.apache.doris.thrift.TLanceIndexJobResultCode;
import org.apache.doris.thrift.TLanceIndexJobTerminationReport;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TLanceIndexTerminationProof;
import org.apache.doris.thrift.TLanceScanParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
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
                .setStorageOptions(lanceStorageOptions())
                .setInvocationSecret("a3f1c02d97b64e8fad0c31b9e75d2468");

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
        // The per-dispatch report secret travels with the identity it authenticates.
        Assert.assertTrue(restored.isSetInvocationSecret());
        Assert.assertEquals("a3f1c02d97b64e8fad0c31b9e75d2468", restored.getInvocationSecret());
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
        // dataset carries no storage options at all. The secret is optional on the wire
        // only for generated-code compatibility during a rolling upgrade; the FE always
        // sets it on a fresh dispatch. A dispatch from a job record that predates the
        // admitted-bound snapshot likewise leaves both bound fields unset.
        Assert.assertFalse(restored.isSetPropertiesJson());
        Assert.assertFalse(restored.isSetIfNotExists());
        Assert.assertFalse(restored.isSetIfExists());
        Assert.assertFalse(restored.isSetStorageOptions());
        Assert.assertFalse(restored.isSetInvocationSecret());
        Assert.assertFalse(restored.isSetMaxNumPartitions());
        Assert.assertFalse(restored.isSetMaxNumSubVectors());
        Assert.assertEquals(TLanceIndexMutationType.CREATE, restored.getMutationType());
        Assert.assertEquals("file:///data/ds", restored.getDatasetUri());
    }

    @Test
    public void testLanceIndexJobDispatchCarriesTheAdmittedBoundSnapshot() throws Exception {
        TLanceIndexJobDispatch source = minimalDispatch()
                .setMaxNumPartitions(4096)
                .setMaxNumSubVectors(256);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] bytes = serializer.serialize(source);

        TLanceIndexJobDispatch restored = new TLanceIndexJobDispatch();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

        Assert.assertTrue(restored.isSetMaxNumPartitions());
        Assert.assertEquals(4096, restored.getMaxNumPartitions());
        Assert.assertTrue(restored.isSetMaxNumSubVectors());
        Assert.assertEquals(256, restored.getMaxNumSubVectors());
    }

    @Test
    public void testLanceIndexJobTerminationReportCompactRoundTrip() throws Exception {
        for (TLanceIndexTerminationProof proof : new TLanceIndexTerminationProof[]{
                TLanceIndexTerminationProof.CHILD_REAPED, TLanceIndexTerminationProof.NEVER_LAUNCHED}) {
            TLanceIndexJobTerminationReport source = new TLanceIndexJobTerminationReport()
                    .setJobId(9L)
                    .setDispatchRevision(4L)
                    .setInvocationId("0f1e2d3c-termination")
                    .setBeProcessEpoch(66L)
                    .setProof(proof);

            TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
            byte[] bytes = serializer.serialize(source);

            TLanceIndexJobTerminationReport restored = new TLanceIndexJobTerminationReport();
            new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);

            Assert.assertEquals(9L, restored.getJobId());
            Assert.assertEquals(4L, restored.getDispatchRevision());
            Assert.assertEquals("0f1e2d3c-termination", restored.getInvocationId());
            Assert.assertEquals(66L, restored.getBeProcessEpoch());
            Assert.assertEquals(proof, restored.getProof());
        }
    }

    @Test
    public void testTerminationReportRejectsBytesMissingARequiredField() throws Exception {
        // A termination report without its proof is not a proof at all: the required-field
        // discipline rejects the frame at read time, before any FE logic sees it.
        byte[] withoutProof = terminationReportBytesWithProofValue(-1);
        TLanceIndexJobTerminationReport restored = new TLanceIndexJobTerminationReport();
        try {
            new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, withoutProof);
            Assert.fail("a termination report frame without a proof must fail to deserialize");
        } catch (TException expected) {
            // Required-field validation on read.
        }
    }

    @Test
    public void testUnknownWireProofValueIsRejectedAtReadOrDroppedToNull() throws Exception {
        // An unknown enum value on the wire resolves to null through findByValue. On the
        // termination report the proof is required, so the read itself is rejected; on the
        // result envelope the proof is optional, so the envelope survives with a null proof
        // the FE handler then ignores. Neither path can map an unknown value to a wrong proof.
        byte[] unknownProof = terminationReportBytesWithProofValue(99);
        try {
            new TDeserializer(new TCompactProtocol.Factory()).deserialize(
                    new TLanceIndexJobTerminationReport(), unknownProof);
            Assert.fail("an unknown termination-proof value must fail the required-field check");
        } catch (TException expected) {
            // findByValue(99) is null and the required-field validation rejects the frame.
        }

        TLanceIndexJobReport restored = new TLanceIndexJobReport();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored,
                reportBytesWithProofValue(99));
        // For an object field Java thrift derives isSet from null-ness, so an unknown
        // enum value collapses to "unset": the proof reads null and the FE handler
        // ignores it, while the trusted result code still lands.
        Assert.assertNull(restored.getTerminationProof());
        Assert.assertFalse(restored.isSetTerminationProof());
        Assert.assertEquals(TLanceIndexJobResultCode.NATIVE_OK, restored.getResultCode());
    }

    @Test
    public void testDispatchPayloadBoundsMatchTheBeProtocol() {
        // The FE pre-send validation mirrors the BE-side protocol limits; the numbers are a
        // permanent cross-side contract, pinned here against accidental drift.
        Assert.assertEquals(64, LanceIndexDispatchBounds.MAX_STORAGE_OPTIONS);
        Assert.assertEquals(256, LanceIndexDispatchBounds.MAX_STORAGE_OPTION_KEY_BYTES);
        Assert.assertEquals(4096, LanceIndexDispatchBounds.MAX_STORAGE_OPTION_VALUE_BYTES);
        Assert.assertEquals(512 * 1024, LanceIndexDispatchBounds.MAX_DISPATCH_BYTES);
    }

    @Test
    public void testMaximalLegalDispatchFitsTheFrameBound() throws Exception {
        // A legal maximal fixture: 64 storage options with 256-byte keys and 4096-byte
        // values, and every required string at its durable bound. The compact frame of
        // such a dispatch must fit the 512 KiB protocol bound with room to spare; the
        // measured size (about 280 KiB, dominated by the storage-option values) is the
        // evidence behind the frozen constant.
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < LanceIndexDispatchBounds.MAX_STORAGE_OPTIONS; i++) {
            String suffix = String.valueOf(i);
            options.put(repeat('k', LanceIndexDispatchBounds.MAX_STORAGE_OPTION_KEY_BYTES - suffix.length())
                            + suffix,
                    repeat('v', LanceIndexDispatchBounds.MAX_STORAGE_OPTION_VALUE_BYTES));
        }
        Assert.assertEquals(LanceIndexDispatchBounds.MAX_STORAGE_OPTIONS, options.size());
        TLanceIndexJobDispatch maximal = minimalDispatch()
                .setInvocationId(repeat('i', 256))
                .setIndexName(repeat('n', 64))
                .setColumnName(repeat('c', 1024))
                .setIndexType(repeat('t', 64))
                .setPropertiesJson(repeat('p', 4096))
                .setDatasetUri("s3://" + repeat('u', 1018))
                .setSchemaContractJson(repeat('s', 4096))
                .setMaxNumPartitions(4096)
                .setMaxNumSubVectors(256)
                .setStorageOptions(options);

        int serializedBytes = LanceIndexDispatchBounds.serializedSizeBytes(maximal);
        Assert.assertTrue("maximal legal dispatch is " + serializedBytes + " bytes, past the bound",
                serializedBytes <= LanceIndexDispatchBounds.MAX_DISPATCH_BYTES);
        // And it passes the same pre-send validation the dispatcher runs.
        LanceIndexDispatchBounds.validatePayload(maximal);
    }

    private static String repeat(char c, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(c);
        }
        return builder.toString();
    }

    /**
     * Serializes a termination report frame by hand, with the proof field carrying
     * {@code proofValue}, or omitted entirely when {@code proofValue} is negative.
     * Hand-written frames are the only way to put an unknown enum value or a missing
     * required field on the wire: the generated serializer validates on write.
     */
    private static byte[] terminationReportBytesWithProofValue(int proofValue) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TCompactProtocol protocol = new TCompactProtocol(new TIOStreamTransport(out));
        protocol.writeStructBegin(new TStruct("TLanceIndexJobTerminationReport"));
        protocol.writeFieldBegin(new TField("job_id", TType.I64, (short) 1));
        protocol.writeI64(9L);
        protocol.writeFieldEnd();
        protocol.writeFieldBegin(new TField("dispatch_revision", TType.I64, (short) 2));
        protocol.writeI64(4L);
        protocol.writeFieldEnd();
        protocol.writeFieldBegin(new TField("invocation_id", TType.STRING, (short) 3));
        protocol.writeString("inv");
        protocol.writeFieldEnd();
        protocol.writeFieldBegin(new TField("be_process_epoch", TType.I64, (short) 4));
        protocol.writeI64(66L);
        protocol.writeFieldEnd();
        if (proofValue >= 0) {
            protocol.writeFieldBegin(new TField("proof", TType.I32, (short) 5));
            protocol.writeI32(proofValue);
            protocol.writeFieldEnd();
        }
        protocol.writeFieldStop();
        protocol.writeStructEnd();
        return out.toByteArray();
    }

    /**
     * Serializes a minimal result-envelope frame by hand, with the optional
     * termination-proof field carrying {@code proofValue}. See
     * {@link #terminationReportBytesWithProofValue} for why the frame is hand-written.
     */
    private static byte[] reportBytesWithProofValue(int proofValue) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TCompactProtocol protocol = new TCompactProtocol(new TIOStreamTransport(out));
        protocol.writeStructBegin(new TStruct("TLanceIndexJobReport"));
        protocol.writeFieldBegin(new TField("job_id", TType.I64, (short) 1));
        protocol.writeI64(9L);
        protocol.writeFieldEnd();
        protocol.writeFieldBegin(new TField("dispatch_revision", TType.I64, (short) 2));
        protocol.writeI64(4L);
        protocol.writeFieldEnd();
        protocol.writeFieldBegin(new TField("invocation_id", TType.STRING, (short) 3));
        protocol.writeString("inv");
        protocol.writeFieldEnd();
        protocol.writeFieldBegin(new TField("be_process_epoch", TType.I64, (short) 4));
        protocol.writeI64(66L);
        protocol.writeFieldEnd();
        protocol.writeFieldBegin(new TField("result_code", TType.I32, (short) 5));
        protocol.writeI32(TLanceIndexJobResultCode.NATIVE_OK.getValue());
        protocol.writeFieldEnd();
        protocol.writeFieldBegin(new TField("termination_proof", TType.I32, (short) 9));
        protocol.writeI32(proofValue);
        protocol.writeFieldEnd();
        protocol.writeFieldStop();
        protocol.writeStructEnd();
        return out.toByteArray();
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
                .setTerminationProof(TLanceIndexTerminationProof.CHILD_REAPED)
                .setInvocationSecret("a3f1c02d97b64e8fad0c31b9e75d2468");

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
        // The secret echo is what authenticates the envelope; it round-trips verbatim.
        Assert.assertTrue(restored.isSetInvocationSecret());
        Assert.assertEquals("a3f1c02d97b64e8fad0c31b9e75d2468", restored.getInvocationSecret());
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
        // its own meaning (NONE reason, no message, no advancement, no proof). A report
        // without the secret echo exists only on the wire of a rolling upgrade; the FE
        // treats it as unauthenticated against any record that carries a secret.
        Assert.assertFalse(restored.isSetCompletionReason());
        Assert.assertFalse(restored.isSetSanitizedMessage());
        Assert.assertFalse(restored.isSetExternalMetadataAdvanced());
        Assert.assertFalse(restored.isSetTerminationProof());
        Assert.assertFalse(restored.isSetInvocationSecret());
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
        Assert.assertEquals(3, TLanceIndexTerminationProof.NEVER_LAUNCHED.getValue());

        // NO_TRUSTED_RESULT is FE-side only and must never gain a wire number.
        Assert.assertEquals(12, TLanceIndexJobResultCode.values().length);
        for (TLanceIndexJobResultCode code : TLanceIndexJobResultCode.values()) {
            Assert.assertNotEquals("NO_TRUSTED_RESULT", code.name());
        }
        // BE_PROCESS_EPOCH_GONE is FE-derived (heartbeat epochs) and must never gain one either.
        Assert.assertEquals(3, TLanceIndexTerminationProof.values().length);
        for (TLanceIndexTerminationProof proof : TLanceIndexTerminationProof.values()) {
            Assert.assertNotEquals("BE_PROCESS_EPOCH_GONE", proof.name());
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
        Assert.assertNull(TLanceIndexTerminationProof.findByValue(4));
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
        expectedDispatchIds.put("invocation_secret", 17);
        expectedDispatchIds.put("max_num_partitions", 18);
        expectedDispatchIds.put("max_num_sub_vectors", 19);
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
        expectedReportIds.put("invocation_secret", 10);
        Assert.assertEquals(expectedReportIds, fieldIdsByName(TLanceIndexJobReport.metaDataMap));

        Map<String, Integer> expectedTerminationIds = new HashMap<>();
        expectedTerminationIds.put("job_id", 1);
        expectedTerminationIds.put("dispatch_revision", 2);
        expectedTerminationIds.put("invocation_id", 3);
        expectedTerminationIds.put("be_process_epoch", 4);
        expectedTerminationIds.put("proof", 5);
        Assert.assertEquals(expectedTerminationIds, fieldIdsByName(TLanceIndexJobTerminationReport.metaDataMap));
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
