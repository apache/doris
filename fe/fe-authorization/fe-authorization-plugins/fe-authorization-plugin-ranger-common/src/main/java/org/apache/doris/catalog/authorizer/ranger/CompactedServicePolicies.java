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

package org.apache.doris.catalog.authorizer.ranger;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compacts a downloaded policy snapshot after Ranger's string normalization and before its evaluators
 * retain the resource maps. The pool is local to this construction: neither old snapshots nor a growing
 * global interning table keep obsolete resource descriptors alive.
 */
final class CompactedServicePolicies extends ServicePolicies {
    private static final long serialVersionUID = 1L;

    private CompactedServicePolicies(ServicePolicies source) {
        setServiceName(source.getServiceName());
        setServiceId(source.getServiceId());
        setPolicyVersion(source.getPolicyVersion());
        setPolicyUpdateTime(source.getPolicyUpdateTime());
        setPolicies(source.getPolicies());
        setServiceDef(source.getServiceDef());
        setAuditMode(source.getAuditMode());
        setTagPolicies(source.getTagPolicies());
        setSecurityZones(source.getSecurityZones());
        setPolicyDeltas(source.getPolicyDeltas());
        setServiceConfig(source.getServiceConfig());
    }

    static ServicePolicies wrap(ServicePolicies source, boolean deduplicateStrings) {
        if (source == null) {
            return null;
        }
        CompactedServicePolicies compacted = new CompactedServicePolicies(source);
        if (!deduplicateStrings) {
            compacted.compact();
        }
        return compacted;
    }

    @Override
    public void dedupStrings() {
        super.dedupStrings();
        compact();
    }

    private void compact() {
        ResourcePool pool = new ResourcePool();
        pool.compactPolicies(getPolicies());
        pool.compactDeltas(getPolicyDeltas());
        if (getTagPolicies() != null) {
            pool.compactPolicies(getTagPolicies().getPolicies());
        }
        if (getSecurityZones() != null) {
            for (SecurityZoneInfo zone : getSecurityZones().values()) {
                pool.compactPolicies(zone.getPolicies());
                pool.compactDeltas(zone.getPolicyDeltas());
            }
        }
    }

    private static final class ResourcePool {
        // A service with millions of unique column names must not build a second unbounded index here.
        private static final int MAX_DESCRIPTORS_PER_LEVEL = 16_384;

        private final Map<String, Map<RangerPolicyResource, RangerPolicyResource>> descriptors = new HashMap<>();

        private void compactPolicies(List<RangerPolicy> policies) {
            if (policies == null) {
                return;
            }
            for (RangerPolicy policy : policies) {
                compactPolicy(policy);
            }
        }

        private void compactDeltas(List<RangerPolicyDelta> deltas) {
            if (deltas == null) {
                return;
            }
            for (RangerPolicyDelta delta : deltas) {
                if (delta.getPolicy() != null) {
                    compactPolicy(delta.getPolicy());
                }
            }
        }

        private void compactPolicy(RangerPolicy policy) {
            policy.setResources(compactMap(policy.getResources()));
            List<Map<String, RangerPolicyResource>> additional = policy.getAdditionalResources();
            if (additional == null || additional.isEmpty()) {
                return;
            }
            List<Map<String, RangerPolicyResource>> compacted = new ArrayList<>(additional.size());
            for (Map<String, RangerPolicyResource> resource : additional) {
                compacted.add(compactMap(resource));
            }
            policy.setAdditionalResources(compacted);
        }

        private Map<String, RangerPolicyResource> compactMap(Map<String, RangerPolicyResource> resource) {
            if (resource == null || resource.isEmpty()) {
                return resource;
            }
            for (Map.Entry<String, RangerPolicyResource> entry : resource.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    // Leave malformed resources to Ranger's existing validation/scrubbing path.
                    return resource;
                }
            }
            Map<String, RangerPolicyResource> result = new HashMap<>(resource.size());
            for (Map.Entry<String, RangerPolicyResource> entry : resource.entrySet()) {
                result.put(entry.getKey(), descriptor(entry.getKey(), entry.getValue()));
            }
            // Preserve HashMap for Ranger's hot matcher: other map types increased request allocations.
            return result;
        }

        private RangerPolicyResource descriptor(String level, RangerPolicyResource resource) {
            List<String> values = resource.getValues();
            if (resource.getClass() != RangerPolicyResource.class || values.size() != 1 || values.get(0) == null) {
                return resource;
            }
            Map<RangerPolicyResource, RangerPolicyResource> byValue = descriptors.computeIfAbsent(level,
                    ignored -> new HashMap<>());
            RangerPolicyResource shared = byValue.get(resource);
            if (shared != null) {
                return shared;
            }
            if (byValue.size() == MAX_DESCRIPTORS_PER_LEVEL) {
                return resource;
            }
            // These descriptors are read by the evaluator; retain the exact value, order and flags.
            byValue.put(resource, resource);
            return resource;
        }
    }
}
