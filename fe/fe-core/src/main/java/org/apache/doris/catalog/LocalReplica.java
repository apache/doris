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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TUniqueId;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalReplica extends Replica {
    private static final Logger LOG = LogManager.getLogger(LocalReplica.class);

    @SerializedName(value = "rds", alternate = {"remoteDataSize"})
    private volatile long remoteDataSize = 0;
    @SerializedName(value = "ris", alternate = {"remoteInvertedIndexSize"})
    private Long remoteInvertedIndexSize = 0L;
    @SerializedName(value = "rss", alternate = {"remoteSegmentSize"})
    private Long remoteSegmentSize = 0L;

    private TUniqueId cooldownMetaId;
    private long cooldownTerm = -1;

    public LocalReplica() {
        super();
    }

    public LocalReplica(ReplicaContext context) {
        super(context);
    }

    // for rollup
    // the new replica's version is -1 and last failed version is -1
    public LocalReplica(long replicaId, long backendId, int schemaHash, ReplicaState state) {
        super(replicaId, backendId, schemaHash, state);
    }

    // for create tablet and restore
    public LocalReplica(long replicaId, long backendId, ReplicaState state, long version, int schemaHash) {
        super(replicaId, backendId, state, version, schemaHash);
    }

    public LocalReplica(long replicaId, long backendId, long version, int schemaHash, long dataSize,
            long remoteDataSize, long rowCount, ReplicaState state, long lastFailedVersion, long lastSuccessVersion) {
        super(replicaId, backendId, version, schemaHash, dataSize, remoteDataSize, rowCount, state, lastFailedVersion,
                lastSuccessVersion);
        this.remoteDataSize = remoteDataSize;
    }

    @Override
    public long getRemoteDataSize() {
        return remoteDataSize;
    }

    @Override
    public void setRemoteDataSize(long remoteDataSize) {
        this.remoteDataSize = remoteDataSize;
    }

    @Override
    public Long getRemoteInvertedIndexSize() {
        return remoteInvertedIndexSize;
    }

    @Override
    public void setRemoteInvertedIndexSize(long remoteInvertedIndexSize) {
        this.remoteInvertedIndexSize = remoteInvertedIndexSize;
    }

    @Override
    public Long getRemoteSegmentSize() {
        return remoteSegmentSize;
    }

    @Override
    public void setRemoteSegmentSize(long remoteSegmentSize) {
        this.remoteSegmentSize = remoteSegmentSize;
    }

    @Override
    public TUniqueId getCooldownMetaId() {
        return cooldownMetaId;
    }

    @Override
    public void setCooldownMetaId(TUniqueId cooldownMetaId) {
        this.cooldownMetaId = cooldownMetaId;
    }

    @Override
    public long getCooldownTerm() {
        return cooldownTerm;
    }

    @Override
    public void setCooldownTerm(long cooldownTerm) {
        this.cooldownTerm = cooldownTerm;
    }
}
