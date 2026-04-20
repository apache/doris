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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CloneSnapshotState {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @JsonProperty("from_instance_id")
    private String fromInstanceId;
    @JsonProperty("from_snapshot_id")
    private String fromSnapshotId;
    @JsonProperty("instance_id")
    private String instanceId;
    @JsonProperty("name")
    private String name;
    @JsonProperty("is_read_only")
    private Boolean isReadOnly;
    @JsonProperty("obj_info")
    private ObjInfo objInfo;
    private Cloud.StorageVaultPB vault;
    @JsonProperty("is_successor")
    private Boolean isSuccessor;

    public static class ObjInfo {
        @JsonProperty("ak")
        private String ak;
        @JsonProperty("sk")
        private String sk;
        @JsonProperty("cred_provider_type")
        private String credProviderType;
        @JsonProperty("role_arn")
        private String roleArn;
        @JsonProperty("external_id")
        private String externalId;
        @JsonProperty("bucket")
        private String bucket;
        @JsonProperty("prefix")
        private String prefix;
        @JsonProperty("endpoint")
        private String endpoint;
        @JsonProperty("external_endpoint")
        private String externalEndpoint;
        @JsonProperty("region")
        private String region;
        @JsonProperty("provider")
        private String provider;

        public Cloud.ObjectStoreInfoPB getObjectStoreInfoPB() {
            Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder()
                    .setBucket(bucket).setPrefix(prefix)
                    .setEndpoint(endpoint).setExternalEndpoint(externalEndpoint).setRegion(region)
                    .setProvider(getProvider());
            if (roleArn != null && !roleArn.isEmpty()) {
                builder.setRoleArn(roleArn)
                        .setCredProviderType(getCredProviderType());
                if (externalId != null && !externalId.isEmpty()) {
                    builder.setExternalId(externalId);
                }
            } else {
                builder.setAk(ak).setSk(sk);
            }
            return builder.build();
        }

        public Cloud.CredProviderTypePB getCredProviderType() {
            if (credProviderType == null || credProviderType.isEmpty()) {
                return Cloud.CredProviderTypePB.INSTANCE_PROFILE;
            }

            Cloud.CredProviderTypePB value;
            try {
                value = Cloud.CredProviderTypePB.valueOf(credProviderType);
            } catch (Exception e) {
                throw new IllegalArgumentException("Unknown cred provider type: " + credProviderType);
            }
            if (value != Cloud.CredProviderTypePB.INSTANCE_PROFILE) {
                throw new IllegalArgumentException("Unsupported cred provider type: " + credProviderType);
            }
            return value;
        }

        private Cloud.ObjectStoreInfoPB.Provider getProvider() {
            try {
                return Cloud.ObjectStoreInfoPB.Provider.valueOf(provider);
            } catch (Exception e) {
                throw new IllegalArgumentException("Unknown provider: " + provider);
            }
        }
    }

    public String getFromInstanceId() {
        return fromInstanceId;
    }

    public String getFromSnapshotId() {
        return fromSnapshotId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getName() {
        return name;
    }

    public boolean isReadOnly() {
        return isReadOnly != null && isReadOnly.booleanValue();
    }

    public boolean isSuccessor() {
        return isSuccessor != null && isSuccessor.booleanValue();
    }

    public Cloud.ObjectStoreInfoPB getObjectStoreInfoPB() {
        return getWritableObjectStoreInfoPB();
    }

    public boolean hasStorageVault() {
        return vault != null;
    }

    public Cloud.StorageVaultPB getStorageVaultPB() {
        return vault;
    }

    @JsonProperty("vault")
    public void setVault(JsonNode vaultNode) {
        vault = parseStorageVault(vaultNode);
    }

    private Cloud.StorageVaultPB parseStorageVault(JsonNode vaultNode) {
        if (vaultNode == null || vaultNode.isNull()) {
            return null;
        }
        try {
            Cloud.StorageVaultPB.Builder builder = Cloud.StorageVaultPB.newBuilder();
            JsonNode idNode = vaultNode.get("id");
            if (idNode != null && !idNode.isNull()) {
                builder.setId(idNode.asText());
            }
            JsonNode nameNode = vaultNode.get("name");
            if (nameNode != null && !nameNode.isNull()) {
                builder.setName(nameNode.asText());
            }
            JsonNode objInfoNode = vaultNode.get("obj_info");
            if (objInfoNode != null && !objInfoNode.isNull()) {
                ObjInfo parsedObjInfo = OBJECT_MAPPER.treeToValue(objInfoNode, ObjInfo.class);
                checkObjInfo("vault.obj_info", parsedObjInfo);
                builder.setObjInfo(parsedObjInfo.getObjectStoreInfoPB());
            }
            return builder.build();
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse vault", e);
        }
    }

    private void checkNotNull(String paramName, String param) {
        if (param == null || param.isEmpty()) {
            throw new IllegalArgumentException(paramName + " is null");
        }
    }

    private void checkOptionalNotEmpty(String paramName, String param) {
        if (param != null) {
            checkNotNull(paramName, param);
        }
    }

    private Cloud.ObjectStoreInfoPB getWritableObjectStoreInfoPB() {
        if (objInfo != null) {
            return objInfo.getObjectStoreInfoPB();
        }
        if (vault != null && vault.hasObjInfo()) {
            return vault.getObjInfo();
        }
        return null;
    }

    private void checkObjInfo(String prefix, ObjInfo objInfo) {
        boolean hasRoleArn = objInfo.roleArn != null && !objInfo.roleArn.isEmpty();
        boolean hasAk = objInfo.ak != null && !objInfo.ak.isEmpty();
        boolean hasSk = objInfo.sk != null && !objInfo.sk.isEmpty();
        if (hasRoleArn && (hasAk || hasSk)) {
            throw new IllegalArgumentException(prefix + " cannot set both ak/sk and role_arn");
        }
        objInfo.getCredProviderType();
        if (!hasRoleArn) {
            checkNotNull(prefix + ".ak", objInfo.ak);
            checkNotNull(prefix + ".sk", objInfo.sk);
        }
        checkNotNull(prefix + ".bucket", objInfo.bucket);
        // prefix can be empty
        checkNotNull(prefix + ".endpoint", objInfo.endpoint);
        checkNotNull(prefix + ".external_endpoint", objInfo.externalEndpoint);
        checkNotNull(prefix + ".region", objInfo.region);
        checkNotNull(prefix + ".provider", objInfo.provider);
        objInfo.getProvider();
    }

    private void checkParsedObjInfo(String prefix, Cloud.ObjectStoreInfoPB objInfo) {
        boolean hasRoleArn = objInfo.hasRoleArn() && !objInfo.getRoleArn().isEmpty();
        if (!hasRoleArn) {
            checkNotNull(prefix + ".ak", objInfo.hasAk() ? objInfo.getAk() : null);
            checkNotNull(prefix + ".sk", objInfo.hasSk() ? objInfo.getSk() : null);
        }
        checkNotNull(prefix + ".bucket", objInfo.hasBucket() ? objInfo.getBucket() : null);
        // prefix can be empty
        checkNotNull(prefix + ".endpoint", objInfo.hasEndpoint() ? objInfo.getEndpoint() : null);
        checkNotNull(prefix + ".external_endpoint",
                objInfo.hasExternalEndpoint() ? objInfo.getExternalEndpoint() : null);
        checkNotNull(prefix + ".region", objInfo.hasRegion() ? objInfo.getRegion() : null);
        if (!objInfo.hasProvider() || objInfo.getProvider() == Cloud.ObjectStoreInfoPB.Provider.UNKONWN) {
            throw new IllegalArgumentException(prefix + ".provider is null");
        }
    }

    public void check() {
        checkNotNull("from_instance_id", fromInstanceId);
        checkNotNull("from_snapshot_id", fromSnapshotId);
        checkNotNull("instance_id", instanceId);
        checkNotNull("name", name);
        if (isReadOnly()) {
            throw new IllegalArgumentException("read only clone is not supported yet");
        }
        if (isSuccessor()) {
            if (objInfo != null) {
                throw new IllegalArgumentException("obj_info must be null when is_successor is true");
            }
            if (vault != null) {
                throw new IllegalArgumentException("vault must be null when is_successor is true");
            }
        }
        if (!isSuccessor()) {
            if (objInfo != null && vault != null) {
                throw new IllegalArgumentException("obj_info and vault cannot both be set");
            }
            if (vault != null) {
                checkOptionalNotEmpty("vault.id", vault.hasId() ? vault.getId() : null);
                checkOptionalNotEmpty("vault.name", vault.hasName() ? vault.getName() : null);
                if (!vault.hasObjInfo()) {
                    throw new IllegalArgumentException("vault.obj_info is null");
                }
            }
            if (objInfo != null) {
                checkObjInfo("obj_info", objInfo);
            }
            Cloud.ObjectStoreInfoPB writableObjInfo = getWritableObjectStoreInfoPB();
            // Since read only clone is not supported yet, so objInfo must not be null
            if (writableObjInfo == null) {
                throw new IllegalArgumentException("obj_info is null, it is required for writeable clone");
            }
            checkParsedObjInfo(vault != null ? "vault.obj_info" : "obj_info", writableObjInfo);
        }
    }
}
