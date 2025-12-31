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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class StreamingJobPropertiesTest {

    @Test
    public void testSessionVariables() throws JobException {
        //default
        StreamingJobProperties jobProperties = new StreamingJobProperties(new HashMap<>());
        ConnectContext ctx = InsertTask.makeConnectContext(null, null);
        SessionVariable defaultSessionVar = jobProperties.getSessionVariable(ctx.getSessionVariable());
        Assert.assertEquals(StreamingJobProperties.DEFAULT_JOB_INSERT_TIMEOUT, defaultSessionVar.getInsertTimeoutS());
        Assert.assertEquals(StreamingJobProperties.DEFAULT_JOB_QUERY_TIMEOUT, defaultSessionVar.getQueryTimeoutS());

        // set session var
        ctx = InsertTask.makeConnectContext(null, null);
        SessionVariable userSessionVar = new SessionVariable();
        userSessionVar.setInsertTimeoutS(1);
        userSessionVar.setQueryTimeoutS(2);
        ctx.setSessionVariable(userSessionVar);

        SessionVariable userSessionVarRes = jobProperties.getSessionVariable(ctx.getSessionVariable());
        Assert.assertEquals(1, userSessionVarRes.getInsertTimeoutS());
        Assert.assertEquals(2, userSessionVarRes.getQueryTimeoutS());

        // set session map in job properties
        ctx = InsertTask.makeConnectContext(null, null);
        HashMap<String, String> props = new HashMap<>();
        props.put("session.insert_timeout", "10");
        props.put("session.query_timeout", "20");
        StreamingJobProperties jobPropertiesMap = new StreamingJobProperties(props);
        SessionVariable sessionVarMap = jobPropertiesMap.getSessionVariable(ctx.getSessionVariable());
        Assert.assertEquals(10, sessionVarMap.getInsertTimeoutS());
        Assert.assertEquals(20, sessionVarMap.getQueryTimeoutS());
    }
}
