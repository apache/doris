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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.SourceOffsetProviderFactory;
import org.apache.doris.job.offset.s3.S3EventSourceOffsetProvider;
import org.apache.doris.job.offset.s3.S3SourceOffsetProvider;
import org.apache.doris.nereids.trees.plans.commands.AlterJobCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class StreamingJobPropertiesTest {
    @Test
    public void testS3ProviderSelectionDefaultsToOrderedList() throws AnalysisException {
        StreamingJobProperties orderedList = new StreamingJobProperties(new HashMap<>());
        orderedList.validate();
        Assertions.assertTrue(SourceOffsetProviderFactory.createSourceOffsetProvider("s3", orderedList)
                instanceof S3SourceOffsetProvider);

        HashMap<String, String> props = new HashMap<>();
        props.put("s3.ingestion_mode", "notification");
        props.put("s3.event.source", "sqs");
        props.put("s3.sqs.queue_url", "https://sqs.us-west-2.amazonaws.com/123456789012/events");
        StreamingJobProperties event = new StreamingJobProperties(props);
        event.validate();
        Assertions.assertTrue(SourceOffsetProviderFactory.createSourceOffsetProvider("s3", event)
                instanceof S3EventSourceOffsetProvider);
    }

    @Test
    public void testS3ModeNames() {
        Assertions.assertEquals("ORDERED_LIST", new StreamingJobProperties(Map.of()).getS3IngestionMode());
        Assertions.assertEquals("ORDERED_LIST",
                new StreamingJobProperties(Map.of("s3.ingestion_mode", " ordered_list ")).getS3IngestionMode());
        Assertions.assertTrue(new StreamingJobProperties(
                Map.of("s3.ingestion_mode", "NOTIFICATION")).isS3NotificationMode());
        Assertions.assertTrue(new StreamingJobProperties(
                Map.of("s3.ingestion_mode", "ONE_TIME")).isS3OneTimeMode());
        for (String mode : new String[] {"LEXICAL", "EVENT", "ONCE"}) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> new StreamingJobProperties(Map.of("s3.ingestion_mode", mode)).validate());
        }
    }

    @Test
    public void testNotificationRequiresQueueAndRejectsManualOffset() throws AnalysisException {
        HashMap<String, String> props = new HashMap<>();
        props.put("s3.ingestion_mode", "NOTIFICATION");
        Assertions.assertThrows(AnalysisException.class, () -> new StreamingJobProperties(props).validate());
        props.put("s3.event.source", "SQS");
        Assertions.assertThrows(AnalysisException.class, () -> new StreamingJobProperties(props).validate());
        props.put("s3.sqs.queue_url", "https://sqs.us-west-2.amazonaws.com/123456789012/events");
        new StreamingJobProperties(props).validate();
        props.put("offset", "logs/a.csv");
        Assertions.assertThrows(AnalysisException.class, () -> new StreamingJobProperties(props).validate());
        props.remove("offset");
        props.put("max_interval", "7200");
        Assertions.assertThrows(AnalysisException.class, () -> new StreamingJobProperties(props).validate());
    }


    @Test
    public void testS3OneTimeModeValidationAndAlter() throws Exception {
        StreamingJobProperties properties = new StreamingJobProperties(Map.of("s3.ingestion_mode", " one_time "));
        properties.validate();
        Assertions.assertTrue(properties.isS3OneTimeMode());
        Assertions.assertThrows(AnalysisException.class,
                () -> new StreamingJobProperties(Map.of("s3.ingestion_mode", "invalid")).validate());
        Assertions.assertThrows(AnalysisException.class,
                () -> new StreamingJobProperties(
                        Map.of("s3.ingestion_mode", "ONE_TIME", "offset", "{\"fileName\":\"a.csv\"}"))
                        .validate());

        StreamingInsertJob job = new StreamingInsertJob();
        Deencapsulation.setField(job, "properties", properties.getProperties());
        AlterJobCommand alterBatch = new AlterJobCommand("job", Map.of("s3.max_batch_files", "1"),
                null, null, null, Map.of(), Map.of());
        Deencapsulation.invoke(alterBatch, "validateProps", job);
        AlterJobCommand alterMode = new AlterJobCommand("job", Map.of("s3.ingestion_mode", "ORDERED_LIST"),
                null, null, null, Map.of(), Map.of());
        Assertions.assertThrows(AnalysisException.class,
                () -> Deencapsulation.invoke(alterMode, "validateProps", job));
        AlterJobCommand alterOffset = new AlterJobCommand("job",
                Map.of("offset", "{\"fileName\":\"a.csv\"}"), null, null, null, Map.of(), Map.of());
        Assertions.assertThrows(AnalysisException.class,
                () -> Deencapsulation.invoke(alterOffset, "validateProps", job));
    }

    @Test
    public void testS3OneTimeModeRestoredWithLegacyOffset() throws Exception {
        StreamingInsertJob job = new StreamingInsertJob();
        Deencapsulation.setField(job, "properties", Map.of("s3.ingestion_mode", "ONE_TIME"));
        Deencapsulation.setField(job, "tvfType", "s3");
        job.setOffsetProviderPersist("{\"endFile\":\"data/a.csv\"}");
        job.gsonPostProcess();
        job.setJobStatus(JobStatus.RUNNING);
        Assertions.assertFalse(job.hasMoreDataToConsume());
        Assertions.assertFalse(job.hasReachedEnd());
    }

    /**
     * Simulate FE restart: constructor is called without validate().
     * Before the fix, maxIntervalSecond would be 0 when properties is non-empty,
     * causing streaming tasks to timeout immediately after FE restart.
     */
    @Test
    public void testConstructorParsesPropertiesWithoutValidate() {
        // Case 1: empty properties -> should use defaults
        StreamingJobProperties emptyProps = new StreamingJobProperties(new HashMap<>());
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
                emptyProps.getMaxIntervalSecond());
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_MAX_S3_BATCH_FILES,
                emptyProps.getS3BatchFiles());
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_MAX_S3_BATCH_BYTES,
                emptyProps.getS3BatchBytes());

        // Case 2: explicit max_interval=1 (the bug scenario)
        // Before fix: maxIntervalSecond would be 0 because isEmpty()=false skipped defaults
        HashMap<String, String> props = new HashMap<>();
        props.put("max_interval", "1");
        StreamingJobProperties customProps = new StreamingJobProperties(props);
        Assertions.assertEquals(1L, customProps.getMaxIntervalSecond());

        // Case 3: explicit max_interval=5
        HashMap<String, String> props2 = new HashMap<>();
        props2.put("max_interval", "5");
        StreamingJobProperties customProps2 = new StreamingJobProperties(props2);
        Assertions.assertEquals(5L, customProps2.getMaxIntervalSecond());
        // s3 properties not set -> should use defaults
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_MAX_S3_BATCH_FILES,
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
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
                p.getMaxIntervalSecond());

        // zero value -> fallback to default (must be >= 1)
        HashMap<String, String> props2 = new HashMap<>();
        props2.put("max_interval", "0");
        StreamingJobProperties p2 = new StreamingJobProperties(props2);
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
                p2.getMaxIntervalSecond());

        // negative value -> fallback to default
        HashMap<String, String> props3 = new HashMap<>();
        props3.put("max_interval", "-1");
        StreamingJobProperties p3 = new StreamingJobProperties(props3);
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
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
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND,
                p.getMaxIntervalSecond());
        // but validate() should throw
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, p::validate);
        Assertions.assertTrue(exception.getMessage().contains("max_interval must be at least 1 second"));
    }

    @Test
    public void testSessionVariables() throws JobException {
        //default
        StreamingJobProperties jobProperties = new StreamingJobProperties(new HashMap<>());
        ConnectContext ctx = InsertTask.makeConnectContext(null, null);
        SessionVariable defaultSessionVar = jobProperties.getSessionVariable(ctx.getSessionVariable());
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_JOB_INSERT_TIMEOUT, defaultSessionVar.getInsertTimeoutS());
        Assertions.assertEquals(StreamingJobProperties.DEFAULT_JOB_QUERY_TIMEOUT, defaultSessionVar.getQueryTimeoutS());

        // set session var
        ctx = InsertTask.makeConnectContext(null, null);
        SessionVariable userSessionVar = new SessionVariable();
        userSessionVar.setInsertTimeoutS(1);
        userSessionVar.setQueryTimeoutS(2);
        ctx.setSessionVariable(userSessionVar);

        SessionVariable userSessionVarRes = jobProperties.getSessionVariable(ctx.getSessionVariable());
        Assertions.assertEquals(1, userSessionVarRes.getInsertTimeoutS());
        Assertions.assertEquals(2, userSessionVarRes.getQueryTimeoutS());

        // set session map in job properties
        ctx = InsertTask.makeConnectContext(null, null);
        HashMap<String, String> props = new HashMap<>();
        props.put("session.insert_timeout", "10");
        props.put("session.query_timeout", "20");
        StreamingJobProperties jobPropertiesMap = new StreamingJobProperties(props);
        SessionVariable sessionVarMap = jobPropertiesMap.getSessionVariable(ctx.getSessionVariable());
        Assertions.assertEquals(10, sessionVarMap.getInsertTimeoutS());
        Assertions.assertEquals(20, sessionVarMap.getQueryTimeoutS());
    }
}
