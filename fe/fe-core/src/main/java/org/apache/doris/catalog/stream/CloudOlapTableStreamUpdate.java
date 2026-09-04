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

package org.apache.doris.catalog.stream;

import org.apache.doris.cloud.proto.Cloud;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.Map;

/** A statement snapshot and its desired Cloud Table Stream offsets. */
public class CloudOlapTableStreamUpdate extends AbstractTableStreamUpdate {
    private final Cloud.TableStreamIdentityPB identity;
    private final Map<Long, Cloud.TableStreamPartitionUpdatePB> partitionUpdates;

    public CloudOlapTableStreamUpdate(Cloud.TableStreamIdentityPB identity,
            Map<Long, Cloud.TableStreamPartitionUpdatePB> partitionUpdates) {
        this.identity = Preconditions.checkNotNull(identity);
        this.partitionUpdates = new LinkedHashMap<>(partitionUpdates);
    }

    public Cloud.TableStreamIdentityPB getIdentity() {
        return identity;
    }

    public Map<Long, Cloud.TableStreamPartitionUpdatePB> getPartitionUpdates() {
        return ImmutableMap.copyOf(partitionUpdates);
    }

    public Cloud.TableStreamUpdatePB toProto() {
        return Cloud.TableStreamUpdatePB.newBuilder()
                .setIdentity(identity)
                .addAllPartitionUpdates(partitionUpdates.values())
                .build();
    }

    @Override
    public void merge(AbstractTableStreamUpdate other) {
        Preconditions.checkArgument(other instanceof CloudOlapTableStreamUpdate,
                "Cannot merge local and Cloud Table Stream updates");
        CloudOlapTableStreamUpdate cloudOther = (CloudOlapTableStreamUpdate) other;
        Preconditions.checkArgument(identity.equals(cloudOther.identity),
                "Cannot merge different Cloud Table Streams");
        cloudOther.partitionUpdates.forEach((partitionId, update) -> {
            Cloud.TableStreamPartitionUpdatePB existing = partitionUpdates.putIfAbsent(partitionId, update);
            Preconditions.checkArgument(existing == null || existing.equals(update),
                    "Conflicting updates for Cloud Table Stream partition %s", partitionId);
        });
    }
}
