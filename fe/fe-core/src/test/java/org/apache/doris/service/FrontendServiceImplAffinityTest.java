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

package org.apache.doris.service;

import org.apache.doris.resource.ResourceGroupAffinity;
import org.apache.doris.resource.ResourceGroupAffinityPolicy;
import org.apache.doris.resource.ResourceGroupAffinityPolicyFactory;
import org.apache.doris.thrift.TGroupCommitInfo;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class FrontendServiceImplAffinityTest {
    @Test
    public void testUnsetForwardedGroupCommitAffinityDoesNotResolveDecision() {
        TGroupCommitInfo info = new TGroupCommitInfo();
        ForwardedLoadAffinityPolicy policy = new ForwardedLoadAffinityPolicy();

        try (MockedStatic<ResourceGroupAffinityPolicyFactory> mockedFactory =
                Mockito.mockStatic(ResourceGroupAffinityPolicyFactory.class)) {
            mockedFactory.when(ResourceGroupAffinityPolicyFactory::get).thenReturn(policy);

            Assert.assertNull(FrontendServiceImpl.forwardedGroupCommitLoadAffinity(info));
            Assert.assertEquals(0, policy.forwardedLoadDecisionCalls);
        }
    }

    @Test
    public void testForwardedGroupCommitAffinityUsesSetFields() {
        TGroupCommitInfo info = new TGroupCommitInfo();
        info.setLoadAffinityPreferredGroup("rg_a");
        info.setLoadAffinityPolicy(ResourceGroupAffinity.Policy.PREFER_LOCAL.name());
        ForwardedLoadAffinityPolicy policy = new ForwardedLoadAffinityPolicy();

        try (MockedStatic<ResourceGroupAffinityPolicyFactory> mockedFactory =
                Mockito.mockStatic(ResourceGroupAffinityPolicyFactory.class)) {
            mockedFactory.when(ResourceGroupAffinityPolicyFactory::get).thenReturn(policy);

            Assert.assertSame(policy.decision, FrontendServiceImpl.forwardedGroupCommitLoadAffinity(info));
            Assert.assertEquals(1, policy.forwardedLoadDecisionCalls);
            Assert.assertEquals("rg_a", policy.effectivePreferredGroup);
            Assert.assertEquals(ResourceGroupAffinity.Policy.PREFER_LOCAL.name(), policy.policy);
        }
    }

    private static final class ForwardedLoadAffinityPolicy implements ResourceGroupAffinityPolicy {
        private final ResourceGroupAffinity.AffinityDecision decision =
                new ResourceGroupAffinity.AffinityDecision("rg_a", ResourceGroupAffinity.Policy.PREFER_LOCAL, "test");
        private int forwardedLoadDecisionCalls;
        private String effectivePreferredGroup;
        private String policy;

        @Override
        public ResourceGroupAffinity.AffinityDecision forwardedLoadDecision(String effectivePreferredGroup,
                String policy) {
            forwardedLoadDecisionCalls++;
            this.effectivePreferredGroup = effectivePreferredGroup;
            this.policy = policy;
            return decision;
        }
    }
}
