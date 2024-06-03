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

package org.apache.doris.consistency;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.cglib.core.ReflectUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;


public class ConsistencyCheckerTest {

    @Test
    public void testChooseTablets() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Env env = Env.getCurrentEnv();

        List<Long> list = List.of(0L);
        new Expectations(env) {
            {
                env.getInternalCatalog().getDbIds();
                minTimes = 1;
                result = list;
            }
        };

        ConsistencyChecker checker = new ConsistencyChecker();
        Class<? extends ConsistencyChecker> agentBatchTaskClass = checker.getClass();
        Method toAgentTaskRequest = agentBatchTaskClass.getDeclaredMethod("chooseTablets");
        toAgentTaskRequest.setAccessible(true);

        List<Long> ret = (List<Long>) toAgentTaskRequest.invoke(checker);
        Assert.assertEquals(0, ret.size());
        System.out.println(ret);
    }

}
