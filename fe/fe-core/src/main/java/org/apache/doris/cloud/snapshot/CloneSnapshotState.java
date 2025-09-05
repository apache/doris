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
import org.apache.doris.cloud.storage.RemoteBase;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CloneSnapshotState {

    @JsonProperty("from_instance_id")
    private String fromInstanceId;
    @JsonProperty("from_snapshot_id")
    private String fromSnapshotId;
    @JsonProperty("instance_id")
    private String instanceId;
    @JsonProperty("name")
    private String name;
    @JsonProperty("is_read_only")
    private boolean isReadOnly;
    @JsonProperty("obj_info")
    private ObjInfo objInfo;

    public static class ObjInfo {
        @JsonProperty("ak")
        private String ak;
        @JsonProperty("sk")
        private String sk;
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

        public RemoteBase.ObjectInfo getObjInfo() {
            return new RemoteBase.ObjectInfo(Cloud.ObjectStoreInfoPB.Provider.valueOf(provider), ak, sk, bucket,
                    endpoint, region, prefix);
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
        return isReadOnly;
    }

    public RemoteBase.ObjectInfo getObjInfo() {
        return objInfo.getObjInfo();
    }
}
