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

package org.apache.doris.load.update;

import org.apache.doris.analysis.UpdateStmt;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import com.clearspring.analytics.util.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

public class UpdateManagerTest {

    @Test
    public void testDisableConcurrentUpdate(@Injectable UpdateStmt updateStmt,
                                            @Injectable UpdateStmtExecutor updateStmtExecutor) {
        Config.enable_concurrent_update = false;
        Map<Long, List<UpdateStmtExecutor>> tableIdToCurrentUpdate = Maps.newConcurrentMap();
        List<UpdateStmtExecutor> currentUpdate = Lists.newArrayList();
        currentUpdate.add(updateStmtExecutor);
        tableIdToCurrentUpdate.put(new Long(1), currentUpdate);
        UpdateManager updateManager = new UpdateManager();
        Assert.assertFalse(Deencapsulation.getField(updateManager, "enableConcurrentUpdate"));
        Deencapsulation.setField(updateManager, "tableIdToCurrentUpdate", tableIdToCurrentUpdate);
        new Expectations() {
            {
                updateStmt.getTargetTable().getId();
                result = 1;
            }
        };

        try {
            Deencapsulation.invoke(updateManager, "addUpdateExecutor", updateStmt);
            Assert.fail();
        } catch (Exception e) {
            if (e instanceof DdlException) {
                System.out.println(e.getMessage());
            } else {
                throw e;
            }
        }
    }
}
