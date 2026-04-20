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

package org.apache.doris.cloud.snapshot;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CloneSnapshotStateTest {

    @Test
    public void testCheck() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        List<Pair<String, String>> values = new ArrayList<>();

        // normal clone
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"ak\": \"ak_val\",\n"
                + "        \"sk\": \"ak_val\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"ap-beijing\",\n"
                + "        \"provider\": \"COS\"\n"
                + "    }\n"
                + "}", ""));
        // not set from_instance_id
        values.add(Pair.of("{\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"ak\": \"ak_val\",\n"
                + "        \"sk\": \"ak_val\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"ap-beijing\",\n"
                + "        \"provider\": \"COS\"\n"
                + "    }\n"
                + "}", "from_instance_id is null"));
        // from_snapshot_id set empty string
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"ak\": \"ak_val\",\n"
                + "        \"sk\": \"ak_val\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"ap-beijing\",\n"
                + "        \"provider\": \"COS\"\n"
                + "    }\n"
                + "}", "from_snapshot_id is null"));
        // not set is_read_only, default is false
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"obj_info\": {\n"
                + "        \"ak\": \"ak_val\",\n"
                + "        \"sk\": \"ak_val\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"ap-beijing\",\n"
                + "        \"provider\": \"COS\"\n"
                + "    }\n"
                + "}", ""));
        // is_read_only is true, not supported
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": true\n"
                + "}", "read only clone is not supported yet"));
        // obj_info is null
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\"\n"
                + "}", "obj_info is null"));
        // not set obj_info.ak
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"sk\": \"ak_val\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"ap-beijing\",\n"
                + "        \"provider\": \"COS\"\n"
                + "    }\n"
                + "}", "obj_info.ak is null"));
        // obj_info.sk is empty string
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"ak\": \"ak_val\",\n"
                + "        \"sk\": \"\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"ap-beijing\",\n"
                + "        \"provider\": \"COS\"\n"
                + "    }\n"
                + "}", "obj_info.sk is null"));
        // unknown obj_info.provider
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"ak\": \"ak_val\",\n"
                + "        \"sk\": \"sk_val\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"ap-beijing\",\n"
                + "        \"provider\": \"abc\"\n"
                + "    }\n"
                + "}", "Unknown provider"));
        // normal clone with role arn
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"role_arn\": \"arn:aws:iam::123456789012:role/MyRole\",\n"
                + "        \"external_id\": \"snapshot-external-id\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"us-east-1\",\n"
                + "        \"provider\": \"S3\"\n"
                + "    }\n"
                + "}", ""));
        // normal clone with explicit instance profile cred provider type
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"role_arn\": \"arn:aws:iam::123456789012:role/MyRole\",\n"
                + "        \"cred_provider_type\": \"INSTANCE_PROFILE\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"us-east-1\",\n"
                + "        \"provider\": \"S3\"\n"
                + "    }\n"
                + "}", ""));
        // cred_provider_type only supports INSTANCE_PROFILE
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"role_arn\": \"arn:aws:iam::123456789012:role/MyRole\",\n"
                + "        \"cred_provider_type\": \"DEFAULT\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"us-east-1\",\n"
                + "        \"provider\": \"S3\"\n"
                + "    }\n"
                + "}", "Unsupported cred provider type"));
        // mixed ak/sk and role_arn is not allowed
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"ak\": \"ak_val\",\n"
                + "        \"sk\": \"sk_val\",\n"
                + "        \"role_arn\": \"arn:aws:iam::123456789012:role/MyRole\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"us-east-1\",\n"
                + "        \"provider\": \"S3\"\n"
                + "    }\n"
                + "}", "obj_info cannot set both ak/sk and role_arn"));
        // normal rollback with is_read_only false
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"is_successor\": true\n"
                + "}", ""));
        // normal rollback
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_successor\": true\n"
                + "}", ""));
        // rollback set obj_info
        values.add(Pair.of("{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_successor\": true,\n"
                + "    \"obj_info\": {\n"
                + "        \"ak\": \"ak_val\",\n"
                + "        \"sk\": \"sk_val\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"ap-beijing\",\n"
                + "        \"provider\": \"abc\"\n"
                + "    }\n"
                + "}", "obj_info must be null when is_successor is true"));

        for (Pair<String, String> value : values) {
            CloneSnapshotState cloneSnapshotState = mapper.readValue(value.getKey(), CloneSnapshotState.class);
            try {
                cloneSnapshotState.check();
                Assert.assertTrue("expected: " + value.getRight() + ", but success", value.getRight().isEmpty());
            } catch (Exception e) {
                Assert.assertFalse("expected success but got: " + e.getMessage(), value.getRight().isEmpty());
                Assert.assertTrue("expected: " + value.getRight() + ", got: " + e.getMessage(),
                        e.getMessage().contains(value.getRight()));
            }
        }

    }

    @Test
    public void testGetObjInfo() throws JsonProcessingException {
        String value = "{\n"
                + "    \"from_instance_id\": \"115400978\",\n"
                + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                + "    \"instance_id\": \"156952316\",\n"
                + "    \"name\": \"156952316\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"obj_info\": {\n"
                + "        \"ak\": \"ak_val\",\n"
                + "        \"sk\": \"sk_val\",\n"
                + "        \"bucket\": \"bucket_val\",\n"
                + "        \"prefix\": \"prefix_val\",\n"
                + "        \"endpoint\": \"endpoint_val\",\n"
                + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                + "        \"region\": \"ap-beijing\",\n"
                + "        \"provider\": \"COS\"\n"
                + "    }\n"
                + "}";
        CloneSnapshotState cloneSnapshotState = new ObjectMapper().readValue(value, CloneSnapshotState.class);
        cloneSnapshotState.check();
        Cloud.ObjectStoreInfoPB objInfo = cloneSnapshotState.getObjectStoreInfoPB();
        Assert.assertEquals("ak_val", objInfo.getAk());
        Assert.assertEquals("sk_val", objInfo.getSk());
        Assert.assertEquals("bucket_val", objInfo.getBucket());
        Assert.assertEquals("prefix_val", objInfo.getPrefix());
        Assert.assertEquals("endpoint_val", objInfo.getEndpoint());
        Assert.assertEquals("external_endpoint_val", objInfo.getExternalEndpoint());
        Assert.assertEquals("ap-beijing", objInfo.getRegion());
        Assert.assertEquals(Provider.COS, objInfo.getProvider());
    }

    @Test
    public void testVaultMode() throws JsonProcessingException {
        String value = "{\n"
                + "    \"from_instance_id\": \"source_instance\",\n"
                + "    \"from_snapshot_id\": \"snapshot_id\",\n"
                + "    \"instance_id\": \"target_instance\",\n"
                + "    \"name\": \"target_name\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"vault\": {\n"
                + "        \"id\": \"vault_id\",\n"
                + "        \"name\": \"vault_name\",\n"
                + "        \"obj_info\": {\n"
                + "            \"ak\": \"ak_val\",\n"
                + "            \"sk\": \"sk_val\",\n"
                + "            \"bucket\": \"bucket_val\",\n"
                + "            \"prefix\": \"prefix_val\",\n"
                + "            \"endpoint\": \"endpoint_val\",\n"
                + "            \"external_endpoint\": \"external_endpoint_val\",\n"
                + "            \"region\": \"ap-beijing\",\n"
                + "            \"provider\": \"COS\"\n"
                + "        }\n"
                + "    }\n"
                + "}";
        CloneSnapshotState cloneSnapshotState = new ObjectMapper().readValue(value, CloneSnapshotState.class);
        cloneSnapshotState.check();
        Assert.assertEquals("source_instance", cloneSnapshotState.getFromInstanceId());
        Assert.assertEquals("snapshot_id", cloneSnapshotState.getFromSnapshotId());
        Assert.assertEquals("target_instance", cloneSnapshotState.getInstanceId());
        Assert.assertEquals("target_name", cloneSnapshotState.getName());
        Assert.assertTrue(cloneSnapshotState.hasStorageVault());
        Assert.assertEquals("vault_id", cloneSnapshotState.getStorageVaultPB().getId());
        Assert.assertEquals("vault_name", cloneSnapshotState.getStorageVaultPB().getName());

        Cloud.ObjectStoreInfoPB objInfo = cloneSnapshotState.getObjectStoreInfoPB();
        Assert.assertEquals("ak_val", objInfo.getAk());
        Assert.assertEquals("sk_val", objInfo.getSk());
        Assert.assertEquals("bucket_val", objInfo.getBucket());
        Assert.assertEquals("prefix_val", objInfo.getPrefix());
        Assert.assertEquals("endpoint_val", objInfo.getEndpoint());
        Assert.assertEquals("external_endpoint_val", objInfo.getExternalEndpoint());
        Assert.assertEquals("ap-beijing", objInfo.getRegion());
        Assert.assertEquals(Provider.COS, objInfo.getProvider());
    }

    @Test
    public void testVaultModeWithRoleArn() throws JsonProcessingException {
        String value = "{\n"
                + "    \"from_instance_id\": \"source_instance\",\n"
                + "    \"from_snapshot_id\": \"snapshot_id\",\n"
                + "    \"instance_id\": \"target_instance\",\n"
                + "    \"name\": \"target_name\",\n"
                + "    \"is_read_only\": false,\n"
                + "    \"vault\": {\n"
                + "        \"id\": \"vault_id\",\n"
                + "        \"name\": \"vault_name\",\n"
                + "        \"obj_info\": {\n"
                + "            \"role_arn\": \"arn:aws:iam::123456789012:role/MyRole\",\n"
                + "            \"cred_provider_type\": \"INSTANCE_PROFILE\",\n"
                + "            \"external_id\": \"snapshot-external-id\",\n"
                + "            \"bucket\": \"bucket_val\",\n"
                + "            \"prefix\": \"prefix_val\",\n"
                + "            \"endpoint\": \"endpoint_val\",\n"
                + "            \"external_endpoint\": \"external_endpoint_val\",\n"
                + "            \"region\": \"us-east-1\",\n"
                + "            \"provider\": \"S3\"\n"
                + "        }\n"
                + "    }\n"
                + "}";
        CloneSnapshotState cloneSnapshotState = new ObjectMapper().readValue(value, CloneSnapshotState.class);
        cloneSnapshotState.check();
        Assert.assertTrue(cloneSnapshotState.hasStorageVault());
        Cloud.ObjectStoreInfoPB objInfo = cloneSnapshotState.getObjectStoreInfoPB();
        Assert.assertEquals("arn:aws:iam::123456789012:role/MyRole", objInfo.getRoleArn());
        Assert.assertEquals("snapshot-external-id", objInfo.getExternalId());
        Assert.assertEquals(Cloud.CredProviderTypePB.INSTANCE_PROFILE, objInfo.getCredProviderType());
        Assert.assertFalse(objInfo.hasAk());
        Assert.assertFalse(objInfo.hasSk());
        Assert.assertEquals(Provider.S3, objInfo.getProvider());
    }

    @Test
    public void testGetObjInfoWithRoleArn() throws JsonProcessingException {
        String value = "{\n"
                        + "    \"from_instance_id\": \"115400978\",\n"
                        + "    \"from_snapshot_id\": \"0000553fb058e8a20000\",\n"
                        + "    \"instance_id\": \"156952316\",\n"
                        + "    \"name\": \"156952316\",\n"
                        + "    \"is_read_only\": false,\n"
                        + "    \"obj_info\": {\n"
                        + "        \"role_arn\": \"arn:aws:iam::123456789012:role/MyRole\",\n"
                        + "        \"external_id\": \"snapshot-external-id\",\n"
                        + "        \"cred_provider_type\": \"INSTANCE_PROFILE\",\n"
                        + "        \"bucket\": \"bucket_val\",\n"
                        + "        \"prefix\": \"prefix_val\",\n"
                        + "        \"endpoint\": \"endpoint_val\",\n"
                        + "        \"external_endpoint\": \"external_endpoint_val\",\n"
                        + "        \"region\": \"us-east-1\",\n"
                        + "        \"provider\": \"S3\"\n"
                        + "    }\n"
                        + "}";
        CloneSnapshotState cloneSnapshotState = new ObjectMapper().readValue(value, CloneSnapshotState.class);
        cloneSnapshotState.check();
        Cloud.ObjectStoreInfoPB objInfo = cloneSnapshotState.getObjectStoreInfoPB();
        Assert.assertEquals("arn:aws:iam::123456789012:role/MyRole", objInfo.getRoleArn());
        Assert.assertEquals("snapshot-external-id", objInfo.getExternalId());
        Assert.assertEquals(Cloud.CredProviderTypePB.INSTANCE_PROFILE, objInfo.getCredProviderType());
        Assert.assertFalse(objInfo.hasAk());
        Assert.assertFalse(objInfo.hasSk());
        Assert.assertEquals(Provider.S3, objInfo.getProvider());
    }
}
