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

package org.apache.doris.planner;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionPolicy;
import org.apache.doris.resource.BackendSelectionPolicyFactory;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class OlapTableSinkBackendSelectionExplainTest {
    @Test
    public void testExplainSkipsLoadDecisionWhenLoadSelectionDisabled() {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        DisabledLoadSelectionPolicy policy = new DisabledLoadSelectionPolicy();

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            OlapTableSink sink = new OlapTableSink(null, null, Collections.emptyList(), false);
            StringBuilder explain = new StringBuilder();
            Deencapsulation.invoke(sink, "appendSinkSelectionExplain", explain, "");

            Assert.assertEquals("", explain.toString());
            Assert.assertEquals(0, policy.getLoadSelectionHintCalls);
        } finally {
            ConnectContext.remove();
        }
    }

    private static final class DisabledLoadSelectionPolicy implements BackendSelectionPolicy {
        private int getLoadSelectionHintCalls;

        @Override
        public boolean isLoadSelectionEnabled(ConnectContext context) {
            return false;
        }

        @Override
        public BackendSelection.SelectionHint getLoadSelectionHint(ConnectContext context) {
            getLoadSelectionHintCalls++;
            throw new AssertionError("load selection decision should not be resolved when disabled");
        }
    }
}
