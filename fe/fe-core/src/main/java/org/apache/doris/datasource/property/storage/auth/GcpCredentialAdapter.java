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

package org.apache.doris.datasource.property.storage.auth;

import org.apache.doris.cloud.proto.Cloud.GcpCredentialPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.filesystem.auth.GcpCredential;
import org.apache.doris.filesystem.auth.GcpCredentialProviderType;
import org.apache.doris.thrift.TCredential;
import org.apache.doris.thrift.TGcpCredential;
import org.apache.doris.thrift.TGcpCredentialProviderType;
import org.apache.doris.thrift.TObjStorageType;
import org.apache.doris.thrift.TS3StorageParam;

import org.apache.commons.lang3.StringUtils;

/** GCP native credential shared by the Cloud protobuf and BE Thrift paths. */
public final class GcpCredentialAdapter implements ObjCredential {
    private final GcpCredentialProviderType providerType;
    private final String impersonationServiceAccount;

    public GcpCredentialAdapter(GcpCredential credential) {
        providerType = credential.getCredentialProviderType();
        impersonationServiceAccount = credential.getImpersonationServiceAccount();
    }

    @Override
    public void applyTo(ObjectStoreInfoPB.Builder builder) {
        if (!builder.hasProvider()) {
            builder.setProvider(ObjectStoreInfoPB.Provider.GCP);
        }
        GcpCredentialPB.Builder credential = GcpCredentialPB.newBuilder()
                .setCredentialProviderType(providerType == GcpCredentialProviderType.DEFAULT
                        ? GcpCredentialPB.CredentialProviderType.DEFAULT
                        : GcpCredentialPB.CredentialProviderType.COMPUTE_ENGINE);
        if (StringUtils.isNotBlank(impersonationServiceAccount)) {
            credential.setImpersonationServiceAccount(impersonationServiceAccount);
        }
        builder.getCredentialBuilder().setGcpCredential(credential);
    }

    @Override
    public void applyTo(TS3StorageParam param) {
        if (!param.isSetProvider()) {
            param.setProvider(TObjStorageType.GCP);
        }
        TGcpCredential credential = new TGcpCredential()
                .setCredentialProviderType(providerType == GcpCredentialProviderType.DEFAULT
                        ? TGcpCredentialProviderType.DEFAULT
                        : TGcpCredentialProviderType.COMPUTE_ENGINE);
        if (StringUtils.isNotBlank(impersonationServiceAccount)) {
            credential.setImpersonationServiceAccount(impersonationServiceAccount);
        }
        param.setCredential(new TCredential().setGcpCredential(credential));
    }
}
