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

package org.apache.doris.enterprise;

import org.apache.doris.encryption.DataKeyMaterial;
import org.apache.doris.encryption.RootKeyInfo;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KmsException;

import java.net.URI;

public class AwsKmsRootKeyProvider extends KmsRootKeyProvider {
    private static final Logger LOG = LogManager.getLogger(AwsKmsRootKeyProvider.class);
    RootKeyInfo rootKeyInfo;
    private KmsClient kms;

    @Override
    public void init(RootKeyInfo info) {
        AwsCredentialsProvider credProvider;
        String ak = System.getenv("DORIS_TDE_AK");
        String sk = System.getenv("DORIS_TDE_SK");
        LOG.info("init aws kms client, get ak {} system env", ak);
        if (!Strings.isNullOrEmpty(ak) && !Strings.isNullOrEmpty(sk)) {
            credProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(ak, sk)
            );
        } else {
            credProvider = InstanceProfileCredentialsProvider.create();
        }

        try {
            kms = KmsClient.builder()
                    .region(Region.of(info.region))
                    .endpointOverride(URI.create(info.endpoint))
                    .credentialsProvider(credProvider)
                    .build();
        } catch (SdkClientException e) {
            LOG.error("failed to init KMS client: {}", e.getMessage(), e);
            throw new RuntimeException("init KMS client failed", e);
        }

        this.rootKeyInfo = info;
    }

    public void describeKey() {
        try {
            DescribeKeyRequest req = DescribeKeyRequest.builder()
                    .keyId(rootKeyInfo.cmkId).build();
            DescribeKeyResponse resp = kms.describeKey(req);
            KeyMetadata meta = resp.keyMetadata();
            LOG.info("DescribeKey succeeded for keyId {}: state={}, creationDate={}",
                    rootKeyInfo.cmkId, meta.keyState(), meta.creationDate());
        } catch (KmsException kmsEx) {
            LOG.error("KMS exception in describeKey: {}", kmsEx.getMessage(), kmsEx);
            throw new RuntimeException("describeKey failed", kmsEx);
        } catch (Exception e) {
            LOG.error("Unexpected error in describeKey: {}", e.getMessage(), e);
            throw new RuntimeException("describeKey failed", e);
        }
    }

    public byte[] decrypt(byte[] ciphertext) {
        try {
            DecryptRequest req = DecryptRequest.builder()
                    .ciphertextBlob(SdkBytes.fromByteArray(ciphertext))
                    .keyId(rootKeyInfo.cmkId)
                    .build();
            DecryptResponse resp = kms.decrypt(req);
            byte[] plain = resp.plaintext().asByteArray();
            LOG.info("Decrypt succeeded, plaintext length={}", plain.length);
            return plain;
        } catch (KmsException kmsEx) {
            LOG.error("KMS exception in decrypt: {}", kmsEx.getMessage(), kmsEx);
            throw new RuntimeException("decrypt failed", kmsEx);
        } catch (Exception e) {
            LOG.error("Unexpected error in decrypt: {}", e.getMessage(), e);
            throw new RuntimeException("decrypt failed", e);
        }
    }

    public DataKeyMaterial generateSymmetricDataKey(int length) {
        try {
            GenerateDataKeyRequest req = GenerateDataKeyRequest.builder()
                    .keyId(rootKeyInfo.cmkId)
                    .numberOfBytes(length)
                    .build();
            GenerateDataKeyResponse resp = kms.generateDataKey(req);
            LOG.info("Generated new data key for keyId {}", rootKeyInfo.cmkId);
            return new DataKeyMaterial(resp.plaintext().asByteArray(),
                resp.ciphertextBlob().asByteArray());
        } catch (KmsException e) {
            LOG.error("KMS error during generateDataKey: {}", e.getMessage(), e);
            throw new RuntimeException("generateSymmetricDataKey KMS error", e);
        } catch (Exception e) {
            LOG.error("Unexpected error during generateDataKey: {}", e.getMessage(), e);
            throw new RuntimeException("generateSymmetricDataKey failed", e);
        }
    }
}
