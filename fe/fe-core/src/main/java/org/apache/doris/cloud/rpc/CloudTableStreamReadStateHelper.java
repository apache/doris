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

package org.apache.doris.cloud.rpc;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Fetches and validates one Cloud Table Stream read snapshot. */
public final class CloudTableStreamReadStateHelper {
    private CloudTableStreamReadStateHelper() {
    }

    public static Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>>
            getReadStates(Map<Cloud.TableStreamIdentityPB, Set<Long>> requestedPartitions)
            throws UserException {
        Cloud.GetTableStreamReadStateRequest.Builder request =
                Cloud.GetTableStreamReadStateRequest.newBuilder()
                        .setCloudUniqueId(Config.cloud_unique_id)
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached());
        requestedPartitions.forEach((identity, partitionIds) -> request.addBindings(
                Cloud.TableStreamPartitionSetPB.newBuilder()
                        .setIdentity(identity)
                        .addAllPartitionIds(partitionIds)));

        Cloud.GetTableStreamReadStateResponse response;
        try {
            response = MetaServiceProxy.getInstance().getTableStreamReadState(request.build());
        } catch (RpcException e) {
            throw new UserException("Failed to get Cloud Table Stream read state: " + e.getMessage(), e);
        }
        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new UserException("Failed to get Cloud Table Stream read state: "
                    + response.getStatus().getMsg());
        }
        return validateResponse(requestedPartitions, response);
    }

    private static Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>>
            validateResponse(Map<Cloud.TableStreamIdentityPB, Set<Long>> requestedPartitions,
                    Cloud.GetTableStreamReadStateResponse response) throws UserException {
        Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>> readStates =
                new LinkedHashMap<>();
        for (Cloud.TableStreamReadBindingResultPB binding : response.getBindingsList()) {
            if (!binding.hasIdentity() || !requestedPartitions.containsKey(binding.getIdentity())) {
                throw new UserException("MetaService returned an unexpected Cloud Table Stream binding");
            }
            Map<Long, Cloud.TableStreamPartitionReadStatePB> partitionStates = new LinkedHashMap<>();
            for (Cloud.TableStreamPartitionReadStatePB state : binding.getPartitionStatesList()) {
                if (!state.hasPartitionId() || partitionStates.put(state.getPartitionId(), state) != null) {
                    throw new UserException("MetaService returned duplicate or invalid partition state");
                }
            }
            if (!partitionStates.keySet().equals(requestedPartitions.get(binding.getIdentity()))) {
                throw new UserException("MetaService returned an incomplete Cloud Table Stream binding");
            }
            if (readStates.put(binding.getIdentity(), partitionStates) != null) {
                throw new UserException("MetaService returned a duplicate Cloud Table Stream binding");
            }
        }
        if (!readStates.keySet().equals(requestedPartitions.keySet())) {
            throw new UserException("MetaService did not return all Cloud Table Stream bindings");
        }
        return readStates;
    }
}
