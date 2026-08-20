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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StorageVault;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CreateStorageVaultCommandTest extends TestWithFeService {
    private String vaultName;

    @Override
    protected void runBeforeAll() throws Exception {
        vaultName = "hdfs_nereids";
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testValidateNormal() throws Exception {
        Env env = Env.getCurrentEnv();
        AccessControllerManager accessManager = env.getAccessManager();
        AccessControllerManager spyAcm = Mockito.spy(accessManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Config.cloud_unique_id = "not_empty_nereids";
        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "hdfs")
                .build();
        CreateStorageVaultCommand command = new CreateStorageVaultCommand(true, vaultName, properties);
        Assertions.assertDoesNotThrow(() -> command.validate());
        Assertions.assertEquals(vaultName, command.getVaultName());
        Assertions.assertEquals(StorageVault.StorageVaultType.HDFS, command.getVaultType());

        // testUnsupportedResourceType
        ImmutableMap<String, String> properties1 = ImmutableMap.<String, String>builder()
                .put("type", "hadoop")
                .build();
        CreateStorageVaultCommand command1 = new CreateStorageVaultCommand(true, vaultName, properties1);
        Assertions.assertThrows(AnalysisException.class, () -> command1.validate());
        Config.cloud_unique_id = "";
    }

    @Test
    public void testS3ExpressStorageVaultDerivesEndpointFromRegion() throws Exception {
        Env env = Env.getCurrentEnv();
        AccessControllerManager accessManager = env.getAccessManager();
        AccessControllerManager spyAcm = Mockito.spy(accessManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Config.cloud_unique_id = "not_empty_nereids";
        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "S3")
                .put("provider", "S3EXPRESS")
                .put("s3.region", "us-west-2")
                .put("s3.bucket", "doris-data--usw2-az1--x-s3")
                .put("s3.root.path", "doris/warehouse")
                .put("s3.access_key", "ak")
                .put("s3.secret_key", "sk")
                .put("s3_validity_check", "false")
                .build();
        CreateStorageVaultCommand command = new CreateStorageVaultCommand(
                true, "s3_express_vault", properties);

        Assertions.assertDoesNotThrow(command::validate);
        Assertions.assertEquals("https://s3.us-west-2.amazonaws.com",
                command.getProperties().get(S3ResourceCompat.ENDPOINT));
        StorageVault vault = Assertions.assertDoesNotThrow(() -> StorageVault.fromCommand(command));
        Assertions.assertEquals("https://s3.us-west-2.amazonaws.com",
                vault.getCopiedProperties().get(S3ResourceCompat.ENDPOINT));
        Config.cloud_unique_id = "";
    }

    @Test
    public void testRegularS3StorageVaultStillRequiresEndpoint() throws Exception {
        Env env = Env.getCurrentEnv();
        AccessControllerManager accessManager = env.getAccessManager();
        AccessControllerManager spyAcm = Mockito.spy(accessManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Config.cloud_unique_id = "not_empty_nereids";
        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "S3")
                .put("provider", "S3")
                .put("s3.region", "us-west-2")
                .put("s3.bucket", "regular-s3-bucket")
                .put("s3.root.path", "doris/warehouse")
                .put("s3.access_key", "ak")
                .put("s3.secret_key", "sk")
                .put("s3_validity_check", "false")
                .build();
        CreateStorageVaultCommand command = new CreateStorageVaultCommand(
                true, "regular_s3_vault", properties);

        Assertions.assertDoesNotThrow(command::validate);
        DdlException exception = Assertions.assertThrows(
                DdlException.class, () -> StorageVault.fromCommand(command));
        Assertions.assertEquals("Missing [s3.endpoint] in properties.", exception.getDetailMessage());
        Config.cloud_unique_id = "";
    }
}
