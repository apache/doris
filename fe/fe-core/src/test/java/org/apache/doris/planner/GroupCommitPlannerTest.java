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

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TQueryGlobals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class GroupCommitPlannerTest {
    @Test
    void testRefreshExecPlanFragmentParamsUsesCurrentSessionGlobals() {
        TQueryGlobals templateGlobals = new TQueryGlobals();
        templateGlobals.setNowString("1970-01-01 00:00:00");
        templateGlobals.setTimestampMs(0L);
        templateGlobals.setTimeZone("UTC");
        TPipelineFragmentParams templateParams = new TPipelineFragmentParams();
        templateParams.setQueryGlobals(templateGlobals);
        TPipelineFragmentParamsList template = new TPipelineFragmentParamsList();
        template.addToParamsList(templateParams);

        ConnectContext context = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setTimeZone("Asia/Shanghai");
        sessionVariable.setLcTimeNames("zh_CN");
        context.setSessionVariable(sessionVariable);

        TPipelineFragmentParamsList refreshed =
                GroupCommitPlanner.refreshExecPlanFragmentParams(template, context);
        TQueryGlobals refreshedGlobals = refreshed.getParamsList().get(0).getQueryGlobals();

        Assertions.assertNotSame(template, refreshed);
        Assertions.assertEquals(0L, templateGlobals.getTimestampMs());
        Assertions.assertFalse(templateGlobals.isSetNanoSeconds());
        Assertions.assertNotEquals(0L, refreshedGlobals.getTimestampMs());
        Assertions.assertTrue(refreshedGlobals.isSetNanoSeconds());
        Assertions.assertEquals("Asia/Shanghai", refreshedGlobals.getTimeZone());
        Assertions.assertEquals("zh_CN", refreshedGlobals.getLcTimeNames());
    }
}
