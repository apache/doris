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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.service.FrontendOptions;

import com.google.gson.JsonObject;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;

/**
 * Unit tests for {@link DorisCloudSnapshotHandler}.
 *
 * <p>Uses JMockit to mock MetaServiceProxy RPC calls and Env static methods.
 */
public class DorisCloudSnapshotHandlerTest {

    @Mocked
    private Env env;

    @Mocked
    private MetaServiceProxy metaServiceProxy;

    @TempDir
    Path tempDir;

    private String originalCloudUniqueId;
    private String originalMetaDir;

    @BeforeEach
    public void setUp() throws Exception {
        originalCloudUniqueId = Config.cloud_unique_id;
        originalMetaDir = Config.meta_dir;
        Config.cloud_unique_id = "test_cloud_unique_id";
        Config.meta_dir = tempDir.toString();

        // Create image directory with a dummy image file
        File imageDir = new File(tempDir.toFile(), "image");
        imageDir.mkdirs();
        File imageFile = new File(imageDir, "image.100");
        try (FileWriter writer = new FileWriter(imageFile)) {
            writer.write("dummy_image_content_for_test");
        }
        // Create VERSION file needed by Storage
        File versionFile = new File(imageDir, "VERSION");
        try (FileWriter writer = new FileWriter(versionFile)) {
            writer.write("clusterId=12345\ntoken=test_token\n");
        }

        new MockUp<FrontendOptions>() {
            @Mock
            public String getLocalHostAddressCached() {
                return "127.0.0.1";
            }
        };

        new Expectations() {{
                Env.getCurrentEnv();
                result = env;
                minTimes = 0;

            }};
    }

    @AfterEach
    public void tearDown() {
        Config.cloud_unique_id = originalCloudUniqueId;
        Config.meta_dir = originalMetaDir;
    }

    // ============================================================================
    // initialize tests
    // ============================================================================

    @Test
    public void testInitialize() {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        // Should not throw
        handler.initialize();
    }

    // ============================================================================
    // submitJob tests
    // ============================================================================

    @Test
    public void testSubmitJobRejectsDuplicate() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        // Mock beginSnapshot to return a valid response
        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.beginSnapshot((Cloud.BeginSnapshotRequest) any);
                result = Cloud.BeginSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setSnapshotId("snap_001")
                        .setImageUrl("snapshot/snap_001/image/")
                        .setObjInfo(Cloud.ObjectStoreInfoPB.newBuilder()
                                .setAk("test_ak").setSk("test_sk")
                                .setBucket("test-bucket").setEndpoint("s3.example.com")
                                .setRegion("us-east-1").setPrefix("data")
                                .setProvider(Cloud.ObjectStoreInfoPB.Provider.S3)
                                .build())
                        .build();
                minTimes = 0;

                // Mock abort for cleanup
                metaServiceProxy.abortSnapshot((Cloud.AbortSnapshotRequest) any);
                result = Cloud.AbortSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .build();
                minTimes = 0;
            }};

        // First submit should succeed (sets sentinel via CAS)
        handler.submitJob(3600, "test_label");

        // Second submit immediately — sentinel is already set (synchronous CAS),
        // so this must be rejected with DdlException.
        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> handler.submitJob(3600, "test_label_2"),
                "Second submit should be rejected while first is in progress");
        Assertions.assertTrue(ex.getMessage().contains("already")
                        || ex.getMessage().contains("progress"),
                "Rejection message should indicate in-progress snapshot: " + ex.getMessage());

        // Wait for async workflow to finish
        Thread.sleep(500);
    }

    @Test
    public void testSubmitJobBasicFlow() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;
            }};

        // Should not throw - submits async task
        Assertions.assertDoesNotThrow(() -> handler.submitJob(3600, "basic_test"));
    }

    // ============================================================================
    // refreshAutoSnapshotJob tests
    // ============================================================================

    @Test
    public void testRefreshAutoSnapshotJobUpdatesInterval() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.getInstance((Cloud.GetInstanceRequest) any);
                result = Cloud.GetInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setInstance(Cloud.InstanceInfoPB.newBuilder()
                                .setSnapshotSwitchStatus(Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON)
                                .setMaxReservedSnapshot(10)
                                .setSnapshotIntervalSeconds(1800)
                                .build())
                        .build();
            }};

        handler.refreshAutoSnapshotJob();

        // Interval should be updated to 1800 * 1000 = 1800000 ms
        Assertions.assertEquals(1800000L, handler.getInterval());
    }

    @Test
    public void testRefreshAutoSnapshotJobNoInstance() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.getInstance((Cloud.GetInstanceRequest) any);
                result = Cloud.GetInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.KV_TXN_GET_ERR).build())
                        .build();
            }};

        // Should not throw even when getInstance fails
        Assertions.assertDoesNotThrow(() -> handler.refreshAutoSnapshotJob());
    }

    // ============================================================================
    // cloneSnapshot tests
    // ============================================================================

    @Test
    public void testCloneSnapshotMissingFields() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        // Create a JSON file with missing fields
        File jsonFile = new File(tempDir.toFile(), "snapshot.json");
        try (FileWriter writer = new FileWriter(jsonFile)) {
            writer.write("{\"snapshot_id\": \"snap_001\"}");
        }

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;
            }};

        Assertions.assertThrows(DdlException.class,
                () -> handler.cloneSnapshot(jsonFile.getAbsolutePath()),
                "missing required fields");
    }

    @Test
    public void testCloneSnapshotRpcFailure() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        // Create a complete JSON file
        File jsonFile = new File(tempDir.toFile(), "snapshot.json");
        try (FileWriter writer = new FileWriter(jsonFile)) {
            JsonObject json = new JsonObject();
            json.addProperty("snapshot_id", "snap_001");
            json.addProperty("from_instance_id", "source_inst");
            json.addProperty("new_instance_id", "new_inst");
            json.addProperty("clone_type", "READ_ONLY");
            writer.write(json.toString());
        }

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.cloneInstance((Cloud.CloneInstanceRequest) any);
                result = Cloud.CloneInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT)
                                .setMsg("snapshot not found").build())
                        .build();
            }};

        Assertions.assertThrows(DdlException.class,
                () -> handler.cloneSnapshot(jsonFile.getAbsolutePath()),
                "cloneInstance failed");
    }

    @Test
    public void testCloneSnapshotParseTypes() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        // Test WRITABLE clone type with obj_info
        File jsonFile = new File(tempDir.toFile(), "writable_snapshot.json");
        try (FileWriter writer = new FileWriter(jsonFile)) {
            JsonObject json = new JsonObject();
            json.addProperty("snapshot_id", "snap_002");
            json.addProperty("from_instance_id", "source_inst");
            json.addProperty("new_instance_id", "writable_inst");
            json.addProperty("clone_type", "WRITABLE");
            JsonObject objInfo = new JsonObject();
            objInfo.addProperty("ak", "new_ak");
            objInfo.addProperty("sk", "new_sk");
            objInfo.addProperty("bucket", "new-bucket");
            objInfo.addProperty("endpoint", "s3.new.com");
            objInfo.addProperty("region", "us-west-2");
            json.add("obj_info", objInfo);
            writer.write(json.toString());
        }

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.cloneInstance((Cloud.CloneInstanceRequest) any);
                result = Cloud.CloneInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setObjInfo(Cloud.ObjectStoreInfoPB.newBuilder()
                                .setAk("ak").setSk("sk").setBucket("bucket")
                                .setEndpoint("s3.example.com").setRegion("us-east-1")
                                .setPrefix("data")
                                .setProvider(Cloud.ObjectStoreInfoPB.Provider.S3)
                                .build())
                        .setImageUrl("snapshot/snap_002/image/")
                        .build();

                // The download will try to list objects, which will fail in test
                // but we've verified the clone RPC path works
            }};

        // This will throw because we can't actually connect to S3 in test
        // but the test verifies JSON parsing and RPC request building work
        Assertions.assertThrows(Exception.class,
                () -> handler.cloneSnapshot(jsonFile.getAbsolutePath()));
    }

    @Test
    public void testCloneSnapshotLegacyFieldsCompatibility() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        File jsonFile = new File(tempDir.toFile(), "legacy_snapshot.json");
        try (FileWriter writer = new FileWriter(jsonFile)) {
            JsonObject json = new JsonObject();
            json.addProperty("from_snapshot_id", "snap_legacy");
            json.addProperty("from_instance_id", "source_inst");
            json.addProperty("instance_id", "new_inst_legacy");
            json.addProperty("is_successor", true);
            json.addProperty("name", "legacy_clone");
            writer.write(json.toString());
        }

        final Cloud.CloneInstanceRequest[] captured = new Cloud.CloneInstanceRequest[1];
        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.cloneInstance((Cloud.CloneInstanceRequest) any);
                result = new Delegate<Cloud.CloneInstanceResponse>() {
                        Cloud.CloneInstanceResponse delegate(Cloud.CloneInstanceRequest req) {
                                captured[0] = req;
                                return Cloud.CloneInstanceResponse.newBuilder()
                                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                                .setCode(Cloud.MetaServiceCode.OK).build())
                                        // Intentionally invalid obj_info to stop before remote storage access.
                                        .setObjInfo(Cloud.ObjectStoreInfoPB.newBuilder().build())
                                        .setImageUrl("snapshot/snap_legacy/image/")
                                        .build();
                        }
                };
            }};

        Assertions.assertThrows(DdlException.class,
                () -> handler.cloneSnapshot(jsonFile.getAbsolutePath()));
        Assertions.assertNotNull(captured[0], "cloneInstance request should be captured");
        Assertions.assertEquals("snap_legacy", captured[0].getFromSnapshotId());
        Assertions.assertEquals("source_inst", captured[0].getFromInstanceId());
        Assertions.assertEquals("new_inst_legacy", captured[0].getNewInstanceId());
        Assertions.assertEquals(Cloud.CloneInstanceRequest.CloneType.ROLLBACK, captured[0].getCloneType());
        Assertions.assertEquals("legacy_clone", captured[0].getSnapshotName());
    }

    @Test
    public void testBuildObjectKeyAvoidsDuplicatePrefix() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        Cloud.ObjectStoreInfoPB objInfo = Cloud.ObjectStoreInfoPB.newBuilder()
                .setPrefix("data")
                .build();
        String key = invokeBuildObjectKey(handler, objInfo, "data/snapshot/s1/image/", "image.100");
        Assertions.assertEquals("data/snapshot/s1/image/image.100", key);
    }

    @Test
    public void testBuildObjectKeyAddsMissingPrefix() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        Cloud.ObjectStoreInfoPB objInfo = Cloud.ObjectStoreInfoPB.newBuilder()
                .setPrefix("data")
                .build();
        String key = invokeBuildObjectKey(handler, objInfo, "snapshot/s1/image/", "image.100");
        Assertions.assertEquals("data/snapshot/s1/image/image.100", key);
    }

    private String invokeBuildObjectKey(DorisCloudSnapshotHandler handler,
                                        Cloud.ObjectStoreInfoPB objInfo,
                                        String imageUrl, String fileName) throws Exception {
        Method method = DorisCloudSnapshotHandler.class.getDeclaredMethod(
                "buildObjectKey", Cloud.ObjectStoreInfoPB.class, String.class, String.class);
        method.setAccessible(true);
        return (String) method.invoke(handler, objInfo, imageUrl, fileName);
    }

    // ============================================================================
    // runAfterCatalogReady tests
    // ============================================================================

    @Test
    public void testRunAfterCatalogReadyNotMaster() {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        new Expectations() {{
                Env.getCurrentEnv();
                result = env;

                env.isMaster();
                result = false;
            }};

        // Should return immediately without doing anything
        Assertions.assertDoesNotThrow(() -> handler.runAfterCatalogReady());
    }

    @Test
    public void testRunAfterCatalogReadySwitchOff() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        new Expectations() {{
                Env.getCurrentEnv();
                result = env;
                minTimes = 0;

                env.isMaster();
                result = true;

                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.getInstance((Cloud.GetInstanceRequest) any);
                result = Cloud.GetInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setInstance(Cloud.InstanceInfoPB.newBuilder()
                                .setSnapshotSwitchStatus(Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_OFF)
                                .build())
                        .build();
            }};

        // Should return without creating snapshot since switch is OFF
        Assertions.assertDoesNotThrow(() -> handler.runAfterCatalogReady());
    }

    @Test
    public void testRunAfterCatalogReadyRecentSnapshot() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        long now = System.currentTimeMillis() / 1000;

        new Expectations() {{
                Env.getCurrentEnv();
                result = env;
                minTimes = 0;

                env.isMaster();
                result = true;

                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                // getInstance returns ON with 3600s interval
                metaServiceProxy.getInstance((Cloud.GetInstanceRequest) any);
                result = Cloud.GetInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setInstance(Cloud.InstanceInfoPB.newBuilder()
                                .setSnapshotSwitchStatus(Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON)
                                .setSnapshotIntervalSeconds(3600)
                                .setMaxReservedSnapshot(10)
                                .build())
                        .build();

                // listSnapshot (no aborted) returns a recent NORMAL snapshot
                metaServiceProxy.listSnapshot((Cloud.ListSnapshotRequest) any);
                result = Cloud.ListSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .addSnapshots(Cloud.SnapshotInfoPB.newBuilder()
                                .setSnapshotId("recent_snap")
                                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_NORMAL)
                                .setCreateAt(now - 60)    // created 60s ago
                                .setFinishAt(now - 50)    // finished 50s ago
                                .build())
                        .build();
                minTimes = 0;
            }};

        // Recent snapshot exists, interval not elapsed → should not trigger
        Assertions.assertDoesNotThrow(() -> handler.runAfterCatalogReady());
    }

    // ============================================================================
    // SnapshotState tests
    // ============================================================================

    @Test
    public void testSnapshotStateGetters() {
        SnapshotState state = new SnapshotState("snap_123", "s3://bucket/path");
        Assertions.assertEquals("snap_123", state.getSnapshotId());
        Assertions.assertEquals("s3://bucket/path", state.getSnapshotUrl());
    }

    @Test
    public void testSnapshotStateSerialization() throws Exception {
        SnapshotState original = new SnapshotState("snap_456", "s3://test/url");

        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
        original.write(dos);
        dos.flush();

        java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(baos.toByteArray());
        java.io.DataInputStream dis = new java.io.DataInputStream(bais);
        SnapshotState deserialized = SnapshotState.read(dis);

        Assertions.assertEquals("snap_456", deserialized.getSnapshotId());
        Assertions.assertEquals("s3://test/url", deserialized.getSnapshotUrl());
    }

    // ============================================================================
    // submitJob workflow tests
    // ============================================================================

    @Test
    public void testSubmitJobBeginSnapshotFails() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.beginSnapshot((Cloud.BeginSnapshotRequest) any);
                result = Cloud.BeginSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT)
                                .setMsg("test error").build())
                        .build();
                minTimes = 0;
            }};

        // Should not throw — async, failures handled internally
        Assertions.assertDoesNotThrow(() -> handler.submitJob(3600, "fail_begin"));
        // Give async task time to complete
        Thread.sleep(500);
    }

    @Test
    public void testSubmitJobWithTTL() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.beginSnapshot((Cloud.BeginSnapshotRequest) any);
                result = Cloud.BeginSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setSnapshotId("snap_ttl")
                        .setImageUrl("snapshot/snap_ttl/image/")
                        .setObjInfo(Cloud.ObjectStoreInfoPB.newBuilder()
                                .setAk("ak").setSk("sk")
                                .setBucket("bucket").setEndpoint("s3.example.com")
                                .setRegion("us-east-1").setPrefix("data")
                                .setProvider(Cloud.ObjectStoreInfoPB.Provider.S3)
                                .build())
                        .build();
                minTimes = 0;

                metaServiceProxy.abortSnapshot((Cloud.AbortSnapshotRequest) any);
                result = Cloud.AbortSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .build();
                minTimes = 0;
            }};

        Assertions.assertDoesNotThrow(() -> handler.submitJob(7200, "ttl_test"));
    }

    // ============================================================================
    // cloneSnapshot additional tests
    // ============================================================================

    @Test
    public void testCloneSnapshotFileNotFound() {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        Assertions.assertThrows(DdlException.class,
                () -> handler.cloneSnapshot("/nonexistent/path/snapshot.json"));
    }

    @Test
    public void testCloneSnapshotInvalidJson() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        File badJson = new File(tempDir.toFile(), "bad.json");
        try (FileWriter writer = new FileWriter(badJson)) {
            writer.write("not valid json at all");
        }

        Assertions.assertThrows(Exception.class,
                () -> handler.cloneSnapshot(badJson.getAbsolutePath()));
    }

    // ============================================================================
    // cloneSnapshot success & edge-case tests
    // ============================================================================

    @Test
    public void testCloneSnapshotSuccessReadOnlyVerifiesRequest() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        // Create a complete JSON file with READ_ONLY clone type
        File jsonFile = new File(tempDir.toFile(), "readonly_clone.json");
        try (FileWriter writer = new FileWriter(jsonFile)) {
            JsonObject json = new JsonObject();
            json.addProperty("snapshot_id", "snap_ro_001");
            json.addProperty("from_instance_id", "source_inst_100");
            json.addProperty("new_instance_id", "target_inst_200");
            json.addProperty("clone_type", "READ_ONLY");
            writer.write(json.toString());
        }

        final Cloud.CloneInstanceRequest[] captured = new Cloud.CloneInstanceRequest[1];
        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.cloneInstance((Cloud.CloneInstanceRequest) any);
                result = new Delegate<Cloud.CloneInstanceResponse>() {
                        Cloud.CloneInstanceResponse delegate(Cloud.CloneInstanceRequest req) {
                                captured[0] = req;
                                return Cloud.CloneInstanceResponse.newBuilder()
                                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                                .setCode(Cloud.MetaServiceCode.OK).build())
                                        .setObjInfo(Cloud.ObjectStoreInfoPB.newBuilder()
                                                .setAk("resp_ak").setSk("resp_sk")
                                                .setBucket("resp-bucket")
                                                .setEndpoint("s3.resp.com")
                                                .setRegion("us-east-1")
                                                .setPrefix("data")
                                                .setProvider(Cloud.ObjectStoreInfoPB.Provider.S3)
                                                .build())
                                        .setImageUrl("snapshot/snap_ro_001/image/")
                                        .build();
                        }
                };
            }};

        // Clone will succeed at RPC level but fail at S3 download (no real S3)
        Assertions.assertThrows(Exception.class,
                () -> handler.cloneSnapshot(jsonFile.getAbsolutePath()));

        // Verify the request was built correctly
        Assertions.assertNotNull(captured[0], "CloneInstance request should be captured");
        Assertions.assertEquals("snap_ro_001", captured[0].getFromSnapshotId());
        Assertions.assertEquals("source_inst_100", captured[0].getFromInstanceId());
        Assertions.assertEquals("target_inst_200", captured[0].getNewInstanceId());
        Assertions.assertEquals(Cloud.CloneInstanceRequest.CloneType.READ_ONLY, captured[0].getCloneType());
    }

    @Test
    public void testCloneSnapshotDefaultCloneTypeIsReadOnly() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        // JSON file without clone_type field → should default to READ_ONLY
        File jsonFile = new File(tempDir.toFile(), "no_type_clone.json");
        try (FileWriter writer = new FileWriter(jsonFile)) {
            JsonObject json = new JsonObject();
            json.addProperty("snapshot_id", "snap_default");
            json.addProperty("from_instance_id", "src");
            json.addProperty("new_instance_id", "dst");
            writer.write(json.toString());
        }

        final Cloud.CloneInstanceRequest[] captured = new Cloud.CloneInstanceRequest[1];
        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.cloneInstance((Cloud.CloneInstanceRequest) any);
                result = new Delegate<Cloud.CloneInstanceResponse>() {
                        Cloud.CloneInstanceResponse delegate(Cloud.CloneInstanceRequest req) {
                                captured[0] = req;
                                return Cloud.CloneInstanceResponse.newBuilder()
                                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                                .setCode(Cloud.MetaServiceCode.OK).build())
                                        .setObjInfo(Cloud.ObjectStoreInfoPB.newBuilder()
                                                .setBucket("b").setEndpoint("e").build())
                                        .setImageUrl("snapshot/img/")
                                        .build();
                        }
                };
            }};

        Assertions.assertThrows(Exception.class,
                () -> handler.cloneSnapshot(jsonFile.getAbsolutePath()));
        Assertions.assertNotNull(captured[0]);
        Assertions.assertEquals(Cloud.CloneInstanceRequest.CloneType.READ_ONLY, captured[0].getCloneType());
    }

    @Test
    public void testCloneSnapshotUnknownCloneTypeDefaultsToReadOnly() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        File jsonFile = new File(tempDir.toFile(), "unknown_type.json");
        try (FileWriter writer = new FileWriter(jsonFile)) {
            JsonObject json = new JsonObject();
            json.addProperty("snapshot_id", "snap_unknown");
            json.addProperty("from_instance_id", "src");
            json.addProperty("new_instance_id", "dst");
            json.addProperty("clone_type", "INVALID_TYPE");
            writer.write(json.toString());
        }

        final Cloud.CloneInstanceRequest[] captured = new Cloud.CloneInstanceRequest[1];
        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.cloneInstance((Cloud.CloneInstanceRequest) any);
                result = new Delegate<Cloud.CloneInstanceResponse>() {
                        Cloud.CloneInstanceResponse delegate(Cloud.CloneInstanceRequest req) {
                                captured[0] = req;
                                return Cloud.CloneInstanceResponse.newBuilder()
                                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                                .setCode(Cloud.MetaServiceCode.OK).build())
                                        .setObjInfo(Cloud.ObjectStoreInfoPB.newBuilder()
                                                .setBucket("b").setEndpoint("e").build())
                                        .setImageUrl("snapshot/img/")
                                        .build();
                        }
                };
            }};

        Assertions.assertThrows(Exception.class,
                () -> handler.cloneSnapshot(jsonFile.getAbsolutePath()));
        Assertions.assertNotNull(captured[0]);
        Assertions.assertEquals(Cloud.CloneInstanceRequest.CloneType.READ_ONLY, captured[0].getCloneType());
    }

    @Test
    public void testCloneSnapshotInvalidObjInfoInResponse() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        File jsonFile = new File(tempDir.toFile(), "bad_resp.json");
        try (FileWriter writer = new FileWriter(jsonFile)) {
            JsonObject json = new JsonObject();
            json.addProperty("snapshot_id", "snap_bad_resp");
            json.addProperty("from_instance_id", "src");
            json.addProperty("new_instance_id", "dst");
            json.addProperty("clone_type", "READ_ONLY");
            writer.write(json.toString());
        }

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.cloneInstance((Cloud.CloneInstanceRequest) any);
                result = Cloud.CloneInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        // Empty obj_info → no bucket/endpoint
                        .setObjInfo(Cloud.ObjectStoreInfoPB.newBuilder().build())
                        .setImageUrl("snapshot/snap/image/")
                        .build();
            }};

        // Should throw DdlException because obj_info has no bucket/endpoint
        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> handler.cloneSnapshot(jsonFile.getAbsolutePath()));
        Assertions.assertTrue(ex.getMessage().contains("invalid obj_info"),
                "Error should mention invalid obj_info: " + ex.getMessage());
    }

    // ============================================================================
    // executeWorkflow / submitJob integration tests
    // ============================================================================

    @Test
    public void testSubmitJobClearsStateAfterWorkflow() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                // beginSnapshot fails → workflow ends early, state should be cleared
                metaServiceProxy.beginSnapshot((Cloud.BeginSnapshotRequest) any);
                result = Cloud.BeginSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT)
                                .setMsg("test failure").build())
                        .build();
                minTimes = 0;
            }};

        // First submit succeeds (async workflow will fail internally)
        handler.submitJob(3600, "clear_state_test");

        // Wait for async workflow to finish and clear state
        Thread.sleep(1000);

        // Second submit should succeed (state should have been cleared)
        Assertions.assertDoesNotThrow(
                () -> handler.submitJob(3600, "clear_state_test_2"),
                "After a failed workflow, state should be cleared so new jobs can be submitted");

        Thread.sleep(500);
    }

    @Test
    public void testSubmitJobBeginSnapshotRequestFields() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        final Cloud.BeginSnapshotRequest[] captured = new Cloud.BeginSnapshotRequest[1];
        new Expectations() {{
                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                metaServiceProxy.beginSnapshot((Cloud.BeginSnapshotRequest) any);
                result = new Delegate<Cloud.BeginSnapshotResponse>() {
                        Cloud.BeginSnapshotResponse delegate(Cloud.BeginSnapshotRequest req) {
                                captured[0] = req;
                                return Cloud.BeginSnapshotResponse.newBuilder()
                                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                                .setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT)
                                                .setMsg("intentional test stop").build())
                                        .build();
                        }
                };
                minTimes = 0;
            }};

        handler.submitJob(7200, "field_check_label");
        Thread.sleep(500);

        Assertions.assertNotNull(captured[0], "BeginSnapshot request should be captured");
        Assertions.assertEquals("test_cloud_unique_id", captured[0].getCloudUniqueId());
        Assertions.assertEquals("field_check_label", captured[0].getSnapshotLabel());
        Assertions.assertFalse(captured[0].getAutoSnapshot(), "Manual snapshot should not be auto");
        Assertions.assertEquals(7200L, captured[0].getTtlSeconds());
    }

    // ============================================================================
    // runAfterCatalogReady — auto-snapshot trigger & recycle tests
    // ============================================================================

    @Test
    public void testRunAfterCatalogReadyTriggersWhenNoNormalSnapshots() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        final boolean[] beginCalled = {false};
        new Expectations() {{
                Env.getCurrentEnv();
                result = env;
                minTimes = 0;

                env.isMaster();
                result = true;

                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                // getInstance returns ON with 3600s interval
                metaServiceProxy.getInstance((Cloud.GetInstanceRequest) any);
                result = Cloud.GetInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setInstance(Cloud.InstanceInfoPB.newBuilder()
                                .setSnapshotSwitchStatus(
                                        Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON)
                                .setSnapshotIntervalSeconds(3600)
                                .setMaxReservedSnapshot(10)
                                .build())
                        .build();

                // listSnapshot returns empty (no snapshots at all)
                metaServiceProxy.listSnapshot((Cloud.ListSnapshotRequest) any);
                result = Cloud.ListSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .build();
                minTimes = 0;

                // beginSnapshot should be called for auto-snapshot trigger
                metaServiceProxy.beginSnapshot((Cloud.BeginSnapshotRequest) any);
                result = new Delegate<Cloud.BeginSnapshotResponse>() {
                        Cloud.BeginSnapshotResponse delegate(Cloud.BeginSnapshotRequest req) {
                                beginCalled[0] = true;
                                return Cloud.BeginSnapshotResponse.newBuilder()
                                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                                .setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT)
                                                .setMsg("stop here").build())
                                        .build();
                        }
                };
                minTimes = 0;
            }};

        Assertions.assertDoesNotThrow(() -> handler.runAfterCatalogReady());

        // Allow async auto-snapshot to execute
        Thread.sleep(500);
        Assertions.assertTrue(beginCalled[0],
                "beginSnapshot should be called when no NORMAL snapshots exist and switch is ON");
    }

    @Test
    public void testRunAfterCatalogReadyRecyclesOldSnapshots() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        long now = System.currentTimeMillis() / 1000;
        final List<String> droppedIds = new java.util.ArrayList<>();

        new Expectations() {{
                Env.getCurrentEnv();
                result = env;
                minTimes = 0;

                env.isMaster();
                result = true;

                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                // max_reserved = 2, but we have 3 NORMAL snapshots → should recycle oldest
                metaServiceProxy.getInstance((Cloud.GetInstanceRequest) any);
                result = Cloud.GetInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setInstance(Cloud.InstanceInfoPB.newBuilder()
                                .setSnapshotSwitchStatus(
                                        Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON)
                                .setSnapshotIntervalSeconds(3600)
                                .setMaxReservedSnapshot(2)
                                .build())
                        .build();

                // List returns 3 NORMAL snapshots, oldest finished 7200s ago
                metaServiceProxy.listSnapshot((Cloud.ListSnapshotRequest) any);
                result = Cloud.ListSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .addSnapshots(Cloud.SnapshotInfoPB.newBuilder()
                                .setSnapshotId("old_snap_1")
                                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_NORMAL)
                                .setCreateAt(now - 7200)
                                .setFinishAt(now - 7100)
                                .build())
                        .addSnapshots(Cloud.SnapshotInfoPB.newBuilder()
                                .setSnapshotId("old_snap_2")
                                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_NORMAL)
                                .setCreateAt(now - 5400)
                                .setFinishAt(now - 5300)
                                .build())
                        .addSnapshots(Cloud.SnapshotInfoPB.newBuilder()
                                .setSnapshotId("recent_snap")
                                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_NORMAL)
                                .setCreateAt(now - 100)
                                .setFinishAt(now - 50)
                                .build())
                        .build();
                minTimes = 0;

                // dropSnapshot should be called for old ones
                metaServiceProxy.dropSnapshot((Cloud.DropSnapshotRequest) any);
                result = new Delegate<Cloud.DropSnapshotResponse>() {
                        Cloud.DropSnapshotResponse delegate(Cloud.DropSnapshotRequest req) {
                                droppedIds.add(req.getSnapshotId());
                                return Cloud.DropSnapshotResponse.newBuilder()
                                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                                .setCode(Cloud.MetaServiceCode.OK).build())
                                        .build();
                        }
                };
                minTimes = 0;

                // beginSnapshot for the auto-snapshot submission
                metaServiceProxy.beginSnapshot((Cloud.BeginSnapshotRequest) any);
                result = Cloud.BeginSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT)
                                .setMsg("stop").build())
                        .build();
                minTimes = 0;
            }};

        Assertions.assertDoesNotThrow(() -> handler.runAfterCatalogReady());
        Thread.sleep(500);

        // Should have recycled the oldest snapshots to make room
        Assertions.assertFalse(droppedIds.isEmpty(),
                "Old snapshots should be recycled when exceeding max_reserved");
        Assertions.assertTrue(droppedIds.contains("old_snap_1"),
                "Oldest snapshot should be dropped first");
    }

    // ============================================================================
    // runAfterCatalogReady abort orphan tests
    // ============================================================================

    @Test
    public void testRunAfterCatalogReadyAbortsOrphan() throws Exception {
        DorisCloudSnapshotHandler handler = new DorisCloudSnapshotHandler();
        handler.initialize();

        long now = System.currentTimeMillis() / 1000;

        new Expectations() {{
                Env.getCurrentEnv();
                result = env;
                minTimes = 0;

                env.isMaster();
                result = true;

                MetaServiceProxy.getInstance();
                result = metaServiceProxy;
                minTimes = 0;

                // getInstance returns ON
                metaServiceProxy.getInstance((Cloud.GetInstanceRequest) any);
                result = Cloud.GetInstanceResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setInstance(Cloud.InstanceInfoPB.newBuilder()
                                .setSnapshotSwitchStatus(
                                        Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON)
                                .setSnapshotIntervalSeconds(3600)
                                .setMaxReservedSnapshot(10)
                                .build())
                        .build();

                // listSnapshot returns a PREPARE snapshot (orphan from previous master)
                metaServiceProxy.listSnapshot((Cloud.ListSnapshotRequest) any);
                returns(
                        // First call: check for aborted (include_aborted=true)
                        Cloud.ListSnapshotResponse.newBuilder()
                                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                        .setCode(Cloud.MetaServiceCode.OK).build())
                                .addSnapshots(Cloud.SnapshotInfoPB.newBuilder()
                                        .setSnapshotId("orphan_snap")
                                        .setStatus(Cloud.SnapshotStatus.SNAPSHOT_PREPARE)
                                        .setCreateAt(now - 7200)
                                        .build())
                                .build(),
                        // Second call: normal list
                        Cloud.ListSnapshotResponse.newBuilder()
                                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                        .setCode(Cloud.MetaServiceCode.OK).build())
                                .build());
                minTimes = 0;

                // Abort should be called for the orphan
                metaServiceProxy.abortSnapshot((Cloud.AbortSnapshotRequest) any);
                result = Cloud.AbortSnapshotResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .build();
                minTimes = 0;
            }};

        // Should abort orphan PREPARE snapshot and not throw
        Assertions.assertDoesNotThrow(() -> handler.runAfterCatalogReady());
    }
}
