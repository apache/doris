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


import org.apache.doris.catalog.HdfsStorageVault;
import org.apache.doris.catalog.StorageVault;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.MetaServiceResponseStatus;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.ImmutableMap;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HdfsStorageVaultTest {

    @Before
    public void setUp() {
        Config.cloud_unique_id = "cloud_unique_id";
        Config.meta_service_endpoint = "127.0.0.1:20121";
    }

    StorageVault createHdfsVault(String name, Map<String, String> properties) throws DdlException {
        HdfsStorageVault vault = new HdfsStorageVault(name, false);
        vault.modifyProperties(properties);
        return vault;
    }

    @Test
    public void testAlterMetaServiceNormal() throws DdlException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.AlterObjStoreInfoResponse
                    alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
                Cloud.AlterObjStoreInfoResponse.Builder resp = Cloud.AlterObjStoreInfoResponse.newBuilder();
                resp.setStatus(MetaServiceResponseStatus.newBuilder().build());
                resp.setStorageVaultId("1");
                return resp.build();
            }
        };
        StorageVault vault = createHdfsVault("hdfs", ImmutableMap.of(
                "type", "hdfs",
                "path", "abs/"));
        vault.alterMetaService();
    }

    @Test
    public void testAlterMetaServiceWithDuplicateName() throws DdlException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            private Set<String> existed = new HashSet<>();
            @Mock
            public Cloud.AlterObjStoreInfoResponse
                    alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
                Cloud.AlterObjStoreInfoResponse.Builder resp = Cloud.AlterObjStoreInfoResponse.newBuilder();
                MetaServiceResponseStatus.Builder status = MetaServiceResponseStatus.newBuilder();
                if (existed.contains(request.getHdfs().getVaultName())) {
                    status.setCode(MetaServiceCode.ALREADY_EXISTED);
                } else {
                    status.setCode(MetaServiceCode.OK);
                    existed.add(request.getHdfs().getVaultName());
                }
                resp.setStatus(status.build());
                resp.setStorageVaultId("1");
                return resp.build();
            }
        };
        StorageVault vault = createHdfsVault("hdfs", ImmutableMap.of(
                "type", "hdfs",
                "path", "abs/"));
        vault.alterMetaService();
        Assertions.assertThrows(DdlException.class,
                () -> {
                    vault.alterMetaService();
                });
    }

    @Test
    public void testAlterMetaServiceWithMissingFiels() throws DdlException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.AlterObjStoreInfoResponse
                    alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
                Cloud.AlterObjStoreInfoResponse.Builder resp = Cloud.AlterObjStoreInfoResponse.newBuilder();
                if (!request.getHdfs().hasVaultName() || request.getHdfs().getVaultName().isEmpty()) {
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
                "path", "abs/"));
        Assertions.assertThrows(DdlException.class,
                () -> {
                    vault.alterMetaService();
                });
    }

    @Test
    public void testAlterMetaServiceIfNotExists() throws DdlException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            private Set<String> existed = new HashSet<>();
            @Mock
            public Cloud.AlterObjStoreInfoResponse
                    alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
                Cloud.AlterObjStoreInfoResponse.Builder resp = Cloud.AlterObjStoreInfoResponse.newBuilder();
                MetaServiceResponseStatus.Builder status = MetaServiceResponseStatus.newBuilder();
                if (existed.contains(request.getHdfs().getVaultName())) {
                    status.setCode(MetaServiceCode.ALREADY_EXISTED);
                } else {
                    status.setCode(MetaServiceCode.OK);
                    existed.add(request.getHdfs().getVaultName());
                }
                resp.setStatus(status.build());
                resp.setStorageVaultId("1");
                return resp.build();
            }
        };
        StorageVault vault = new HdfsStorageVault("name", true);
        vault.modifyProperties(ImmutableMap.of(
                "type", "hdfs",
                "path", "abs/"));
        vault.alterMetaService();
    }
}
