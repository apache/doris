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

package org.apache.doris.cloud.catalog;

import org.apache.doris.analysis.CreateStorageVaultStmt;
import org.apache.doris.analysis.SetDefaultStorageVaultStmt;
import org.apache.doris.catalog.HdfsStorageVault;
import org.apache.doris.catalog.StorageVault;
import org.apache.doris.catalog.StorageVaultMgr;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.AlterObjStoreInfoRequest.Operation;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.MetaServiceResponseStatus;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HdfsStorageVaultTest {
    private static final Logger LOG = LogManager.getLogger(HdfsStorageVaultTest.class);
    private StorageVaultMgr mgr = new StorageVaultMgr(new SystemInfoService());

    @BeforeAll
    public static void setUp() throws Exception {
        Config.cloud_unique_id = "cloud_unique_id";
        Config.meta_service_endpoint = "127.0.0.1:20121";
    }

    StorageVault createHdfsVault(String name, Map<String, String> properties) throws Exception {
        CreateStorageVaultStmt stmt = new CreateStorageVaultStmt(false, name, properties);
        stmt.setStorageVaultType(StorageVault.StorageVaultType.HDFS);
        StorageVault vault = StorageVault.fromStmt(stmt);
        return vault;
    }

    @Test
    public void testAlterMetaServiceNormal() throws Exception {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.AlterObjStoreInfoResponse
                    alterStorageVault(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
                Cloud.AlterObjStoreInfoResponse.Builder resp = Cloud.AlterObjStoreInfoResponse.newBuilder();
                resp.setStatus(MetaServiceResponseStatus.newBuilder().build());
                resp.setStorageVaultId("1");
                return resp.build();
            }
        };
        StorageVault vault = createHdfsVault("hdfs", ImmutableMap.of(
                "type", "hdfs",
                "path", "abs/",
                S3Properties.VALIDITY_CHECK, "false",
                HdfsStorageVault.HADOOP_FS_NAME, "default"));
        Map<String, String> properties = vault.getCopiedProperties();
        // To check if the properties is carried correctly
        Assertions.assertEquals(properties.get(HdfsStorageVault.HADOOP_FS_NAME), "default");
        mgr.createHdfsVault(vault);
    }

    @Test
    public void testAlterMetaServiceWithDuplicateName() throws Exception {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            private Set<String> existed = new HashSet<>();
            @Mock
            public Cloud.AlterObjStoreInfoResponse
                    alterStorageVault(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
                Cloud.AlterObjStoreInfoResponse.Builder resp = Cloud.AlterObjStoreInfoResponse.newBuilder();
                MetaServiceResponseStatus.Builder status = MetaServiceResponseStatus.newBuilder();
                if (existed.contains(request.getVault().getName())) {
                    status.setCode(MetaServiceCode.ALREADY_EXISTED);
                } else {
                    status.setCode(MetaServiceCode.OK);
                    existed.add(request.getVault().getName());
                }
                resp.setStatus(status.build());
                resp.setStorageVaultId("1");
                return resp.build();
            }
        };
        StorageVault vault = createHdfsVault("hdfs", ImmutableMap.of(
                "type", "hdfs",
                "path", "abs/",
                S3Properties.VALIDITY_CHECK, "false"));
        mgr.createHdfsVault(vault);
        Assertions.assertThrows(DdlException.class,
                () -> {
                    mgr.createHdfsVault(vault);
                });
    }

    @Test
    public void testAlterMetaServiceWithMissingFiels() throws Exception {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.AlterObjStoreInfoResponse
                    alterStorageVault(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
                Cloud.AlterObjStoreInfoResponse.Builder resp = Cloud.AlterObjStoreInfoResponse.newBuilder();
                if (!request.getVault().hasName() || request.getVault().getName().isEmpty()) {
                    resp.setStatus(MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.INVALID_ARGUMENT).build());
                } else {
                    resp.setStatus(MetaServiceResponseStatus.newBuilder().build());
                }
                resp.setStorageVaultId("1");
                return resp.build();
            }
        };
        StorageVault vault = createHdfsVault("", ImmutableMap.of(
                "type", "hdfs",
                "path", "abs/",
                S3Properties.VALIDITY_CHECK, "false"));
        Assertions.assertThrows(DdlException.class,
                () -> {
                    mgr.createHdfsVault(vault);
                });
    }

    @Test
    public void testAlterMetaServiceIfNotExists() throws Exception {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            private Set<String> existed = new HashSet<>();
            @Mock
            public Cloud.AlterObjStoreInfoResponse
                    alterStorageVault(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
                Cloud.AlterObjStoreInfoResponse.Builder resp = Cloud.AlterObjStoreInfoResponse.newBuilder();
                MetaServiceResponseStatus.Builder status = MetaServiceResponseStatus.newBuilder();
                if (existed.contains(request.getVault().getName())) {
                    status.setCode(MetaServiceCode.ALREADY_EXISTED);
                } else {
                    status.setCode(MetaServiceCode.OK);
                    existed.add(request.getVault().getName());
                }
                resp.setStatus(status.build());
                resp.setStorageVaultId("1");
                return resp.build();
            }
        };
        StorageVault vault = new HdfsStorageVault("name", true, false);
        vault.modifyProperties(ImmutableMap.of(
                "type", "hdfs",
                "path", "abs/",
                S3Properties.VALIDITY_CHECK, "false"));
        mgr.createHdfsVault(vault);
    }

    @Test
    public void testSetDefaultVault() throws Exception {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            private Pair<String, String> defaultVaultInfo;
            private HashSet<String> existed = new HashSet<>();

            @Mock
            public Pair getDefaultStorageVault() {
                return defaultVaultInfo;
            }

            @Mock
            public Cloud.AlterObjStoreInfoResponse
                    alterStorageVault(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
                Cloud.AlterObjStoreInfoResponse.Builder resp = Cloud.AlterObjStoreInfoResponse.newBuilder();
                MetaServiceResponseStatus.Builder status = MetaServiceResponseStatus.newBuilder();
                if (request.getOp() == Operation.ADD_HDFS_INFO) {
                    if (existed.contains(request.getVault().getName())) {
                        status.setCode(MetaServiceCode.ALREADY_EXISTED);
                    } else {
                        status.setCode(MetaServiceCode.OK);
                        existed.add(request.getVault().getName());
                    }
                } else if (request.getOp() == Operation.SET_DEFAULT_VAULT) {
                    if (!existed.contains(request.getVault().getName())) {
                        status.setCode(MetaServiceCode.INVALID_ARGUMENT);
                    } else {
                        this.defaultVaultInfo = Pair.of(request.getVault().getName(), "1");
                        status.setCode(MetaServiceCode.OK);
                    }
                }
                resp.setStatus(status.build());
                resp.setStorageVaultId(String.valueOf(existed.size()));
                return resp.build();
            }
        };
        StorageVault vault = new HdfsStorageVault("name", true, false);
        Assertions.assertThrows(DdlException.class,
                () -> {
                    mgr.setDefaultStorageVault(new SetDefaultStorageVaultStmt("non_existent"));
                });
        vault.modifyProperties(ImmutableMap.of(
                "type", "hdfs",
                "path", "abs/",
                S3Properties.VALIDITY_CHECK, "false"));
        mgr.createHdfsVault(vault);
        Assertions.assertTrue(mgr.getDefaultStorageVault() == null);
        mgr.setDefaultStorageVault(new SetDefaultStorageVaultStmt(vault.getName()));
        Assertions.assertTrue(mgr.getDefaultStorageVault().first.equals(vault.getName()));
    }

    @Test
    public void testCheckConnectivity() {
        try {
            String hadoopFsName = System.getenv("HADOOP_FS_NAME");
            String hadoopUser = System.getenv("HADOOP_USER");

            Assumptions.assumeTrue(!Strings.isNullOrEmpty(hadoopFsName), "HADOOP_FS_NAME isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(hadoopUser), "HADOOP_USER isNullOrEmpty.");

            Map<String, String> properties = new HashMap<>();
            properties.put(HdfsStorageVault.HADOOP_FS_NAME, hadoopFsName);
            properties.put(AuthenticationConfig.HADOOP_USER_NAME, hadoopUser);
            properties.put(HdfsStorageVault.VAULT_PATH_PREFIX, "testCheckConnectivityUtPrefix");

            HdfsStorageVault vault = new HdfsStorageVault("testHdfsVault", false, false);
            vault.modifyProperties(properties);
        } catch (DdlException e) {
            LOG.warn("testCheckConnectivity:", e);
            Assertions.assertTrue(false, e.getMessage());
        }
    }

    @Test
    public void testCheckConnectivityException() {
        Map<String, String> properties = new HashMap<>();
        properties.put(HdfsStorageVault.HADOOP_FS_NAME, "hdfs://localhost:10000");
        properties.put(AuthenticationConfig.HADOOP_USER_NAME, "notExistUser");
        properties.put(HdfsStorageVault.VAULT_PATH_PREFIX, "testCheckConnectivityUtPrefix");

        HdfsStorageVault vault = new HdfsStorageVault("testHdfsVault", false, false);
        Assertions.assertThrows(DdlException.class, () -> {
            vault.modifyProperties(properties);
        });
    }

    @Test
    public void testIgnoreCheckConnectivity() throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(HdfsStorageVault.HADOOP_FS_NAME, "hdfs://localhost:10000");
        properties.put(AuthenticationConfig.HADOOP_USER_NAME, "notExistUser");
        properties.put(HdfsStorageVault.VAULT_PATH_PREFIX, "testCheckConnectivityUtPrefix");
        properties.put(S3Properties.VALIDITY_CHECK, "false");

        HdfsStorageVault vault = new HdfsStorageVault("testHdfsVault", false, false);
        vault.modifyProperties(properties);
    }
}
