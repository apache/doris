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

package org.apache.doris.cloud.storage;

import org.apache.doris.cloud.proto.Cloud;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.time.Instant;

public class DefaultRemoteTest {
    @Test
    public void testRoleArnCredentialsProviderUsesAssumeRole() {
        Cloud.ObjectStoreInfoPB objInfo = Cloud.ObjectStoreInfoPB.newBuilder()
                .setProvider(Cloud.ObjectStoreInfoPB.Provider.S3)
                .setBucket("snapshot-bucket")
                .setEndpoint("s3.us-west-2.amazonaws.com")
                .setRegion("us-west-2")
                .setPrefix("snapshot-prefix")
                .setRoleArn("arn:aws:iam::123456789012:role/snapshot-role")
                .setExternalId("snapshot-external-id")
                .setCredProviderType(Cloud.CredProviderTypePB.INSTANCE_PROFILE)
                .build();

        StsClient mockStsClient = Mockito.mock(StsClient.class);
        Mockito.when(mockStsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
                .thenReturn(AssumeRoleResponse.builder()
                        .credentials(Credentials.builder()
                                .accessKeyId("sts-ak")
                                .secretAccessKey("sts-sk")
                                .sessionToken("sts-token")
                                .expiration(Instant.now().plusSeconds(3600))
                                .build())
                        .build());

        TestableS3Remote remote = new TestableS3Remote(new RemoteBase.ObjectInfo(objInfo), mockStsClient);
        AwsCredentials credentials = remote.getS3CredentialsProvider().resolveCredentials();

        Assertions.assertInstanceOf(AwsSessionCredentials.class, credentials);
        Assertions.assertEquals("sts-ak", credentials.accessKeyId());
        Assertions.assertEquals("sts-sk", credentials.secretAccessKey());
        Assertions.assertEquals("sts-token", ((AwsSessionCredentials) credentials).sessionToken());

        org.mockito.ArgumentCaptor<AssumeRoleRequest> requestCaptor =
                org.mockito.ArgumentCaptor.forClass(AssumeRoleRequest.class);
        Mockito.verify(mockStsClient).assumeRole(requestCaptor.capture());
        Assertions.assertEquals("arn:aws:iam::123456789012:role/snapshot-role",
                requestCaptor.getValue().roleArn());
        Assertions.assertEquals("snapshot-external-id", requestCaptor.getValue().externalId());
    }

    @Test
    public void testStaticCredentialsStillUseBasicProvider() {
        Cloud.ObjectStoreInfoPB objInfo = Cloud.ObjectStoreInfoPB.newBuilder()
                .setProvider(Cloud.ObjectStoreInfoPB.Provider.S3)
                .setBucket("snapshot-bucket")
                .setEndpoint("s3.us-west-2.amazonaws.com")
                .setRegion("us-west-2")
                .setPrefix("snapshot-prefix")
                .setAk("static-ak")
                .setSk("static-sk")
                .build();

        TestableS3Remote remote = new TestableS3Remote(new RemoteBase.ObjectInfo(objInfo), null);

        Assertions.assertInstanceOf(StaticCredentialsProvider.class, remote.getS3CredentialsProvider());
        AwsCredentials credentials = remote.getS3CredentialsProvider().resolveCredentials();
        Assertions.assertEquals("static-ak", credentials.accessKeyId());
        Assertions.assertEquals("static-sk", credentials.secretAccessKey());
    }

    private static class TestableS3Remote extends S3Remote {
        private final StsClient stsClient;

        TestableS3Remote(RemoteBase.ObjectInfo obj, StsClient stsClient) {
            super(obj);
            this.stsClient = stsClient;
        }

        @Override
        protected StsClient createStsClient(software.amazon.awssdk.auth.credentials.AwsCredentialsProvider provider) {
            return stsClient == null ? super.createStsClient(provider) : stsClient;
        }
    }
}
