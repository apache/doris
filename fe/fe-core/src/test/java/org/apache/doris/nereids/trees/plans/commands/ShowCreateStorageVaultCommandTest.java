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

import org.apache.doris.cloud.proto.Cloud.CredProviderTypePB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.datasource.property.storage.S3Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowCreateStorageVaultCommandTest {

    @Test
    void testGcpWorkloadIdentityProperties() {
        ObjectStoreInfoPB objectInfo = ObjectStoreInfoPB.newBuilder()
                .setEndpoint(S3Properties.GCS_XML_ENDPOINT)
                .setRegion("us-central1")
                .setPrefix("test-prefix")
                .setBucket("test-bucket")
                .setProvider(Provider.GCP)
                .setCredProviderType(CredProviderTypePB.GCP_WORKLOAD_IDENTITY)
                .build();

        String createStmt =
                new ShowCreateStorageVaultCommand("test-vault").getObjectCreateStmt(objectInfo);

        Assertions.assertTrue(createStmt.contains(S3Properties.CREDENTIALS_PROVIDER_TYPE));
        Assertions.assertTrue(
                createStmt.contains(S3Properties.GCP_WORKLOAD_IDENTITY_CREDENTIALS_PROVIDER));
        Assertions.assertFalse(createStmt.contains("s3.access_key"));
        Assertions.assertFalse(createStmt.contains("s3.secret_key"));
    }
}
