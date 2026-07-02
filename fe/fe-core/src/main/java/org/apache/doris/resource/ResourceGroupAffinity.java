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

package org.apache.doris.resource;

public final class ResourceGroupAffinity {
    public enum Policy {
        PREFER_LOCAL,
        STRICT,
        RANDOM
    }

    public static final class AffinityDecision {
        private final String effectivePreferredGroup;
        private final Policy effectivePolicy;
        private final String resolveNote;

        public AffinityDecision(String effectivePreferredGroup, Policy effectivePolicy, String resolveNote) {
            this.effectivePreferredGroup = effectivePreferredGroup == null ? "" : effectivePreferredGroup;
            this.effectivePolicy = effectivePolicy == null ? Policy.RANDOM : effectivePolicy;
            this.resolveNote = resolveNote == null ? "" : resolveNote;
        }

        public static AffinityDecision noAffinity() {
            return new AffinityDecision("", Policy.RANDOM, "no_preferred");
        }

        public String getEffectivePreferredGroup() {
            return effectivePreferredGroup;
        }

        public Policy getEffectivePolicy() {
            return effectivePolicy;
        }

        public String getResolveNote() {
            return resolveNote;
        }
    }

    private ResourceGroupAffinity() {
    }
}
