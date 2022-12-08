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

package org.apache.doris.system;

import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Selection policy for building BE nodes
 */
public class BeSelectionPolicy {
    public String cluster = SystemInfoService.DEFAULT_CLUSTER;
    public boolean needScheduleAvailable = false;
    public boolean needQueryAvailable = false;
    public boolean needLoadAvailable = false;
    // Resource tag. Empty means no need to consider resource tag.
    public Set<Tag> resourceTags = Sets.newHashSet();
    // storage medium. null means no need to consider storage medium.
    public TStorageMedium storageMedium = null;
    // Check if disk usage reaches limit. false means no need to check.
    public boolean checkDiskUsage = false;
    // If set to false, do not select backends on same host.
    public boolean allowOnSameHost = false;

    public boolean preferComputeNode = false;
    public int candidateNum = Integer.MAX_VALUE;

    private BeSelectionPolicy() {

    }

    public static class Builder {
        private BeSelectionPolicy policy;

        public Builder() {
            policy = new BeSelectionPolicy();
        }

        public Builder setCluster(String cluster) {
            policy.cluster = cluster;
            return this;
        }

        public Builder needScheduleAvailable() {
            policy.needScheduleAvailable = true;
            return this;
        }

        public Builder needQueryAvailable() {
            policy.needQueryAvailable = true;
            return this;
        }

        public Builder needLoadAvailable() {
            policy.needLoadAvailable = true;
            return this;
        }

        public Builder addTags(Set<Tag> tags) {
            policy.resourceTags.addAll(tags);
            return this;
        }

        public Builder setStorageMedium(TStorageMedium medium) {
            policy.storageMedium = medium;
            return this;
        }

        public Builder needCheckDiskUsage() {
            policy.checkDiskUsage = true;
            return this;
        }

        public Builder allowOnSameHost() {
            policy.allowOnSameHost = true;
            return this;
        }

        public Builder preferComputeNode() {
            policy.preferComputeNode = true;
            return this;
        }

        public Builder assignCandidateNum(int candidateNum) {
            policy.candidateNum = candidateNum;
            return this;
        }

        public BeSelectionPolicy build() {
            return policy;
        }
    }

    private boolean isMatch(Backend backend) {
        // Compute node is only used when preferComputeNode is set.
        if (!preferComputeNode && backend.isComputeNode()) {
            return false;
        }

        if (needScheduleAvailable && !backend.isScheduleAvailable() || needQueryAvailable && !backend.isQueryAvailable()
                || needLoadAvailable && !backend.isLoadAvailable() || !resourceTags.isEmpty() && !resourceTags.contains(
                backend.getLocationTag()) || storageMedium != null && !backend.hasSpecifiedStorageMedium(
                storageMedium)) {
            return false;
        }

        if (checkDiskUsage) {
            if (storageMedium == null && backend.diskExceedLimit()) {
                return false;
            }
            if (storageMedium != null && backend.diskExceedLimitByStorageMedium(storageMedium)) {
                return false;
            }
        }
        return true;
    }

    public List<Backend> getCandidateBackends(ImmutableCollection<Backend> backends) {
        List<Backend> filterBackends = backends.stream().filter(this::isMatch).collect(Collectors.toList());
        Collections.shuffle(filterBackends);
        List<Backend> candidates = new ArrayList<>();
        if (preferComputeNode) {
            int num = 0;
            // pick compute node first
            for (Backend backend : filterBackends) {
                if (backend.isComputeNode()) {
                    if (num >= candidateNum) {
                        break;
                    }
                    candidates.add(backend);
                    num++;
                }
            }
            // fill with some mix node.
            if (num < candidateNum) {
                for (Backend backend : filterBackends) {
                    if (backend.isMixNode()) {
                        if (num >= candidateNum) {
                            break;
                        }
                        candidates.add(backend);
                        num++;
                    }
                }
            }
        } else {
            candidates.addAll(filterBackends);
        }

        return candidates;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("cluster|query|load|schedule|tags|medium: ");
        sb.append(cluster).append("|");
        sb.append(needQueryAvailable).append("|");
        sb.append(needLoadAvailable).append("|");
        sb.append(needScheduleAvailable).append("|");
        sb.append(resourceTags).append("|");
        sb.append(storageMedium);
        return sb.toString();
    }
}
