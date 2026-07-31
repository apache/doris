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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class StreamingJobPropertiesTest {

    /**
     * Simulate FE restart: constructor is called without validate().
     * Before the fix, maxIntervalSecond would be 0 when properties is non-empty,
     * causing streaming tasks to timeout immediately after FE restart.
     */
    @Test
    public void testConstructorParsesPropertiesWithoutValidate() {
        // Case 1: empty properties -> should use defaults
        StreamingJobProperties emptyProps = new StreamingJobProperties(new HashMap<>());
        Assert.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
                emptyProps.getMaxIntervalSecond());
        Assert.assertEquals(StreamingJobProperties.DEFAULT_MAX_S3_BATCH_FILES,
                emptyProps.getS3BatchFiles());
        Assert.assertEquals(StreamingJobProperties.DEFAULT_MAX_S3_BATCH_BYTES,
                emptyProps.getS3BatchBytes());

        // Case 2: explicit max_interval=1 (the bug scenario)
        // Before fix: maxIntervalSecond would be 0 because isEmpty()=false skipped defaults
        HashMap<String, String> props = new HashMap<>();
        props.put("max_interval", "1");
        StreamingJobProperties customProps = new StreamingJobProperties(props);
        Assert.assertEquals(1L, customProps.getMaxIntervalSecond());

        // Case 3: explicit max_interval=5
        HashMap<String, String> props2 = new HashMap<>();
        props2.put("max_interval", "5");
        StreamingJobProperties customProps2 = new StreamingJobProperties(props2);
        Assert.assertEquals(5L, customProps2.getMaxIntervalSecond());
        // s3 properties not set -> should use defaults
        Assert.assertEquals(StreamingJobProperties.DEFAULT_MAX_S3_BATCH_FILES,
                customProps2.getS3BatchFiles());
    }

    /**
     * Constructor should be resilient to bad data (e.g. corrupted metadata),
     * falling back to defaults instead of throwing exceptions.
     */
    @Test
    public void testConstructorHandlesBadValues() {
        // non-numeric value -> fallback to default
        HashMap<String, String> props = new HashMap<>();
        props.put("max_interval", "abc");
        StreamingJobProperties p = new StreamingJobProperties(props);
        Assert.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
                p.getMaxIntervalSecond());

        // zero value -> fallback to default (must be >= 1)
        HashMap<String, String> props2 = new HashMap<>();
        props2.put("max_interval", "0");
        StreamingJobProperties p2 = new StreamingJobProperties(props2);
        Assert.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
                p2.getMaxIntervalSecond());

        // negative value -> fallback to default
        HashMap<String, String> props3 = new HashMap<>();
        props3.put("max_interval", "-1");
        StreamingJobProperties p3 = new StreamingJobProperties(props3);
        Assert.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
                p3.getMaxIntervalSecond());
    }

    /**
     * validate() should still reject bad values with AnalysisException,
     * keeping the strict check for job creation.
     */
    @Test
    public void testValidateStillRejectsBadValues() {
        HashMap<String, String> props = new HashMap<>();
        props.put("max_interval", "0");
        StreamingJobProperties p = new StreamingJobProperties(props);
        // constructor fallback is fine
        Assert.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
                p.getMaxIntervalSecond());
        // but validate() should throw
        Assert.assertThrows(AnalysisException.class, p::validate);
    }

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
