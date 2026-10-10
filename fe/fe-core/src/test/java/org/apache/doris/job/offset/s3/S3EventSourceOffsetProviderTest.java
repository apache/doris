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

package org.apache.doris.job.offset.s3;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.persist.gson.GsonUtils;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class S3EventSourceOffsetProviderTest {
    private static final String QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/123456789012/events";

    private S3EventSourceOffsetProvider provider;
    private Map<String, String> tvfProperties;
    private SqsClient sqsClient;
    private SqsClientBuilder sqsBuilder;
    private MockedStatic<SqsClient> sqsStatic;
    private S3Client s3Client;
    private S3ClientBuilder s3Builder;
    private AwsCredentialsProvider s3CredentialsProvider;
    private MockedStatic<S3Client> s3Static;

    @BeforeEach
    public void setUp() throws Exception {
        tvfProperties = new HashMap<>();
        tvfProperties.put("uri", "s3://bucket/logs/**");
        tvfProperties.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        tvfProperties.put("s3.region", "us-west-2");
        tvfProperties.put("s3.access_key", "test-ak");
        tvfProperties.put("s3.secret_key", "test-sk");
        provider = new S3EventSourceOffsetProvider(QUEUE_URL);
        provider.ensureInitialized(1L, tvfProperties);
        sqsClient = Mockito.mock(SqsClient.class);
        sqsBuilder = Mockito.mock(SqsClientBuilder.class, Mockito.RETURNS_SELF);
        Mockito.when(sqsBuilder.build()).thenReturn(sqsClient);
        sqsStatic = Mockito.mockStatic(SqsClient.class);
        sqsStatic.when(SqsClient::builder).thenReturn(sqsBuilder);
        s3Client = Mockito.mock(S3Client.class);
        s3Builder = Mockito.mock(S3ClientBuilder.class, Mockito.RETURNS_SELF);
        Mockito.when(s3Builder.build()).thenReturn(s3Client);
        Mockito.when(s3Builder.credentialsProvider(Mockito.any(AwsCredentialsProvider.class)))
                .thenAnswer(invocation -> {
                    s3CredentialsProvider = invocation.getArgument(0);
                    return s3Builder;
                });
        Mockito.when(s3Client.serviceClientConfiguration()).thenAnswer(invocation ->
                S3ServiceClientConfiguration.builder().region(Region.US_WEST_2)
                        .credentialsProvider(s3CredentialsProvider).build());
        s3Static = Mockito.mockStatic(S3Client.class);
        s3Static.when(S3Client::builder).thenReturn(s3Builder);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().build());
        Mockito.when(sqsClient.deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class)))
                .thenAnswer(invocation -> {
                    DeleteMessageBatchRequest request = invocation.getArgument(0);
                    return DeleteMessageBatchResponse.builder().successful(request.entries().stream()
                            .map(entry -> DeleteMessageBatchResultEntry.builder().id(entry.id()).build())
                            .collect(Collectors.toList())).build();
                });
        Mockito.when(sqsClient.changeMessageVisibilityBatch(Mockito.any(ChangeMessageVisibilityBatchRequest.class)))
                .thenReturn(ChangeMessageVisibilityBatchResponse.builder().build());
        Mockito.when(sqsClient.getQueueAttributes(Mockito.any(GetQueueAttributesRequest.class)))
                .thenReturn(GetQueueAttributesResponse.builder().attributes(Collections.singletonMap(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0")).build());
    }

    @AfterEach
    public void tearDown() {
        provider.close();
        s3Static.close();
        sqsStatic.close();
    }

    @Test
    public void testClientsAreReusedUntilClosed() {
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Mockito.verify(s3Builder).build();
        Mockito.verify(sqsBuilder).build();
        Mockito.verify(sqsClient, Mockito.never()).close();
        Mockito.verify(s3Client, Mockito.never()).close();

        provider.close();
        provider.close();
        InOrder closeOrder = Mockito.inOrder(sqsClient, s3Client);
        closeOrder.verify(sqsClient).close();
        closeOrder.verify(s3Client).close();
    }

    @Test
    public void testChangedCredentialsReplaceClientsAndReuseReplacement() throws Exception {
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        tvfProperties.put("s3.access_key", "new-ak");
        tvfProperties.put("s3.secret_key", "new-sk");
        provider.ensureInitialized(1L, tvfProperties);
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Mockito.verify(s3Builder, Mockito.times(2)).build();
        Mockito.verify(sqsBuilder, Mockito.times(2)).build();
        Mockito.verify(sqsClient).close();
        Mockito.verify(s3Client).close();
        ArgumentCaptor<AwsCredentialsProvider> credentials = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        Mockito.verify(sqsBuilder, Mockito.times(2)).credentialsProvider(
                (IdentityProvider<? extends AwsCredentialsIdentity>) credentials.capture());
        Assertions.assertEquals("test-ak", credentials.getAllValues().get(0).resolveCredentials().accessKeyId());
        Assertions.assertEquals("new-ak", credentials.getAllValues().get(1).resolveCredentials().accessKeyId());
    }

    @Test
    public void testCloseWaitsForClientCreation() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch closeStarted = new CountDownLatch(1);
            AtomicReference<Future<?>> closeFuture = new AtomicReference<>();
            Mockito.when(sqsBuilder.build()).thenAnswer(invocation -> {
                closeFuture.set(executor.submit(() -> {
                    closeStarted.countDown();
                    provider.close();
                }));
                Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
                Assertions.assertFalse(closeFuture.get().isDone());
                Mockito.verify(s3Client, Mockito.never()).close();
                return sqsClient;
            });
            Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                    .thenAnswer(invocation -> {
                        closeFuture.get().get(5, TimeUnit.SECONDS);
                        return response(filesMessage("late", "logs/a.csv"));
                    });
            provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
            closeFuture.get().get(5, TimeUnit.SECONDS);
            Assertions.assertFalse(provider.hasMoreDataToConsume());
            Mockito.verify(sqsClient).close();
            Mockito.verify(s3Client).close();
            provider.ensureInitialized(1L, tvfProperties);
            provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
            Mockito.verify(sqsBuilder).build();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCloseDuringReceiveDiscardsLateBatch() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                    .thenAnswer(invocation -> {
                        executor.submit(() -> provider.close()).get(5, TimeUnit.SECONDS);
                        Mockito.verify(sqsClient).close();
                        return response(filesMessage("in-flight", "logs/a.csv"));
                    });
            provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
            Assertions.assertFalse(provider.hasMoreDataToConsume());
            Mockito.verify(sqsClient).close();
            Mockito.verify(s3Client).close();
            provider.ensureInitialized(1L, tvfProperties);
            provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
            Mockito.verify(sqsBuilder).build();
            Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCloseDuringReceiveSuppressesRequestFailure() {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenAnswer(invocation -> {
                    provider.close();
                    throw new IllegalStateException("client closed");
                });

        Assertions.assertDoesNotThrow(() -> provider.fetchRemoteMeta(jobProperties(1), tvfProperties));

        Assertions.assertFalse(provider.hasMoreDataToConsume());
        Mockito.verify(sqsClient).close();
        Mockito.verify(s3Client).close();
        Mockito.verify(sqsClient, Mockito.never()).getQueueAttributes(Mockito.any(GetQueueAttributesRequest.class));
    }

    @Test
    public void testClientCreationFailureReleasesFileSystemAndAllowsRetry() {
        Mockito.when(sqsBuilder.build()).thenThrow(new IllegalStateException("client creation failed"))
                .thenReturn(sqsClient);
        Assertions.assertThrows(IllegalStateException.class,
                () -> provider.fetchRemoteMeta(jobProperties(1), tvfProperties));
        Mockito.verify(s3Client).close();

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Mockito.verify(s3Builder, Mockito.times(2)).build();
        Mockito.verify(sqsBuilder, Mockito.times(2)).build();
    }

    @Test
    public void testSqsCloseFailureStillClosesFileSystem() {
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Mockito.doThrow(new IllegalStateException("close failed")).when(sqsClient).close();
        provider.close();
        Mockito.verify(s3Client).close();
    }

    @Test
    public void testNotificationMetadataDoesNotRequireHead() {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(message("literal", record("bucket", "logs/a%2Cb%5B1%5D.csv", 123))));
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        S3EventOffset offset = nextOffset();
        Assertions.assertEquals(Collections.singletonList("logs/a,b[1].csv"), offset.getFiles());
        Assertions.assertEquals("s3://bucket/logs/a,b[1].csv", offset.getFileStatuses().get(0).getPath());
        Assertions.assertEquals(123, offset.getFileStatuses().get(0).getSize());
        Assertions.assertFalse(offset.toSerializedJson().contains("fileStatuses"));
        Mockito.verify(s3Client, Mockito.never()).headObject(Mockito.any(HeadObjectRequest.class));
        Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));
    }

    @Test
    public void testNotificationMatchesFilesystemGlobSyntax() throws Exception {
        tvfProperties.put("uri", "s3://bucket/logs/{001,abc,1..2}.csv");
        provider = new S3EventSourceOffsetProvider(QUEUE_URL);
        provider.ensureInitialized(1L, tvfProperties);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("glob", "logs/001.csv", "logs/abc.csv",
                        "logs/1.csv", "logs/2.csv", "logs/3.csv", "logs//1.csv")));

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        Assertions.assertEquals(Arrays.asList("logs/001.csv", "logs/abc.csv",
                "logs/1.csv", "logs/2.csv"), nextOffset().getFiles());
    }

    @Test
    public void testQuestionMarkGlobPreservesMatchingNotificationsUntilCommit() throws Exception {
        tvfProperties.put("uri", "s3://bucket/logs/file?.csv");
        provider = new S3EventSourceOffsetProvider(QUEUE_URL);
        provider.ensureInitialized(1L, tvfProperties);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("keep", "logs/file1.csv"),
                        filesMessage("skip", "logs/file12.csv")), ReceiveMessageResponse.builder().build());

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        Assertions.assertTrue(provider.hasMoreDataToConsume());
        S3EventOffset offset = nextOffset();
        Assertions.assertEquals(Collections.singletonList("logs/file1.csv"), offset.getFiles());
        Assertions.assertEquals("s3://bucket/logs/file1.csv", offset.getFileStatuses().get(0).getPath());
        ArgumentCaptor<DeleteMessageBatchRequest> deletes = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(sqsClient).deleteMessageBatch(deletes.capture());
        Assertions.assertEquals(Collections.singletonList("skip"), deleteHandles(deletes.getValue()));

        provider.updateOffset(offset);
        provider.onTaskCommitted(1, 1);
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Mockito.verify(sqsClient, Mockito.times(2)).deleteMessageBatch(deletes.capture());
        Assertions.assertEquals(Collections.singletonList("keep"), deleteHandles(deletes.getValue()));
    }

    @Test
    public void testNotificationMatchesLiteralHashInSourceKey() throws Exception {
        tvfProperties.put("uri", "s3://bucket/logs/file#1.csv");
        provider = new S3EventSourceOffsetProvider(QUEUE_URL);
        provider.ensureInitialized(1L, tvfProperties);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("keep", "logs/file%231.csv")));

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        Assertions.assertTrue(provider.hasMoreDataToConsume());
        S3EventOffset offset = nextOffset();
        Assertions.assertEquals(Collections.singletonList("logs/file#1.csv"), offset.getFiles());
        Assertions.assertEquals("s3://bucket/logs/file#1.csv", offset.getFileStatuses().get(0).getPath());
        Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));
    }

    @Test
    public void testDisplayTracksAcceptedAndCommittedBatches() {
        Assertions.assertNull(provider.getShowCurrentOffset());
        Assertions.assertNull(provider.getShowMaxOffset());
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("first", "logs/z.csv", "logs/a%22b.csv"),
                        filesMessage("overflow", "logs/overflow.csv")),
                        ReceiveMessageResponse.builder().build());
        provider.fetchRemoteMeta(jobProperties(2), tvfProperties);
        String endOffset = "{\"fileName\":\"logs/overflow.csv\",\"lagMessages\":0}";
        Assertions.assertNull(provider.getShowCurrentOffset());
        Assertions.assertEquals(endOffset, provider.getShowMaxOffset());
        S3EventOffset offset = nextOffset();
        Assertions.assertEquals(endOffset, provider.getShowMaxOffset());

        Mockito.clearInvocations(sqsClient);
        provider.updateOffset(provider.deserializeOffset(
                provider.getCommitOffsetJson(offset, 1L, Collections.emptyList())));
        provider.onTaskCommitted(3, 3);
        Assertions.assertEquals("{\"fileName\":\"logs/overflow.csv\"}", provider.getShowCurrentOffset());
        Assertions.assertEquals(endOffset, provider.getShowMaxOffset());
        // Compact display must not truncate the durable file list or issue SQS calls.
        Assertions.assertEquals("{\"files\":[\"logs/z.csv\",\"logs/a\\\"b.csv\",\"logs/overflow.csv\"]}",
                provider.getPersistInfo());
        Mockito.verifyNoInteractions(sqsClient);
        provider.fetchRemoteMeta(jobProperties(2), tvfProperties);
        Assertions.assertEquals(endOffset, provider.getShowMaxOffset());
    }

    @Test
    public void testLagMessagesUsesVisibleCountAndSupportsLargeBacklogs() {
        Assertions.assertEquals(-1L, provider.getLagMessages());
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "3000000000");
        attributes.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "12");
        attributes.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, "34");
        Mockito.when(sqsClient.getQueueAttributes(Mockito.any(GetQueueAttributesRequest.class)))
                .thenReturn(GetQueueAttributesResponse.builder().attributes(attributes).build());
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Assertions.assertNull(provider.getShowCurrentOffset());
        Assertions.assertEquals("{\"lagMessages\":3000000000}", provider.getShowMaxOffset());
        Assertions.assertEquals(3000000000L, provider.getLagMessages());
        ArgumentCaptor<GetQueueAttributesRequest> request = ArgumentCaptor.forClass(GetQueueAttributesRequest.class);
        Mockito.verify(sqsClient).getQueueAttributes(request.capture());
        Assertions.assertEquals(QUEUE_URL, request.getValue().queueUrl());
        Assertions.assertEquals(Collections.singletonList(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES),
                request.getValue().attributeNames());
    }

    @Test
    public void testLagMessageFailureClearsStatisticWithoutBlockingBatch() {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("first", "logs/a.csv")));
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Assertions.assertEquals("{\"fileName\":\"logs/a.csv\",\"lagMessages\":0}", provider.getShowMaxOffset());
        Assertions.assertEquals(0L, provider.getLagMessages());
        Mockito.when(sqsClient.getQueueAttributes(Mockito.any(GetQueueAttributesRequest.class)))
                .thenThrow(new IllegalStateException("Access denied"))
                .thenReturn(GetQueueAttributesResponse.builder().attributes(Collections.singletonMap(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "7")).build());

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Assertions.assertEquals("{\"fileName\":\"logs/a.csv\"}", provider.getShowMaxOffset());
        Assertions.assertEquals(-1L, provider.getLagMessages());
        Assertions.assertEquals(Collections.singletonList("logs/a.csv"), nextOffset().getFiles());
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Assertions.assertEquals("{\"fileName\":\"logs/a.csv\",\"lagMessages\":7}", provider.getShowMaxOffset());
        Assertions.assertEquals(7L, provider.getLagMessages());
        Mockito.verify(sqsClient, Mockito.times(1)).receiveMessage(Mockito.any(ReceiveMessageRequest.class));
    }

    @Test
    public void testSourceEventTimestampTracksOnlyCommittedBatchAndIsNotPersisted() {
        String latest = "2026-01-01T11:00:00.000Z";
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(message("first",
                                record("bucket", "logs/a.csv", 1, latest),
                                record("bucket", "logs/b.csv", 1, "2026-01-01T09:00:00.000Z"),
                                record("other", "logs/ignored.csv", 1, "2026-01-01T12:00:00.000Z"),
                                record("bucket", "other/ignored.csv", 1, "2026-01-01T12:00:00.000Z")),
                        message("second", record("bucket", "logs/c.csv", 1, "2026-01-01T11:30:00.000+02:00")),
                        message("overflow",
                                record("bucket", "logs/d.csv", 1, "2026-01-01T11:00:00.000Z"),
                                record("bucket", "logs/e.csv", 1, "2026-01-01T11:00:00.000Z"))));

        Assertions.assertEquals(0L, provider.getLastSourceEventTimestampSeconds());
        provider.fetchRemoteMeta(jobProperties(4), tvfProperties);
        Assertions.assertEquals(0L, provider.getLastSourceEventTimestampSeconds());
        S3EventOffset offset = nextOffset();
        Assertions.assertEquals(0L, provider.getLastSourceEventTimestampSeconds());
        String committedJson = provider.getCommitOffsetJson(offset, 1L, Collections.emptyList());
        Assertions.assertEquals("{\"files\":[\"logs/a.csv\",\"logs/b.csv\",\"logs/c.csv\","
                + "\"logs/d.csv\",\"logs/e.csv\"]}", committedJson);
        provider.updateOffset(provider.deserializeOffset(committedJson));
        provider.onTaskCommitted(5, 5);
        Assertions.assertEquals(Instant.parse(latest).getEpochSecond(), provider.getLastSourceEventTimestampSeconds());
        Mockito.verify(sqsClient, Mockito.never())
                .changeMessageVisibilityBatch(Mockito.any(ChangeMessageVisibilityBatchRequest.class));

        S3EventSourceOffsetProvider restored = new S3EventSourceOffsetProvider(QUEUE_URL);
        restored.restoreFromPersistInfo(provider.getPersistInfo());
        Assertions.assertEquals(provider.getShowCurrentOffset(), restored.getShowCurrentOffset());
        Assertions.assertEquals(0L, restored.getLastSourceEventTimestampSeconds());

        // A newly received batch must not replace the timestamp until it commits.
        // Out-of-order notifications report that batch's time, not a historical maximum.
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("next", "logs/next.csv")));
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Assertions.assertEquals(Instant.parse(latest).getEpochSecond(), provider.getLastSourceEventTimestampSeconds());
        provider.updateOffset(nextOffset());
        provider.onTaskCommitted(1, 1);
        Assertions.assertEquals(Instant.parse("2026-01-01T00:00:00Z").getEpochSecond(),
                provider.getLastSourceEventTimestampSeconds());
    }

    @Test
    public void testMissingSourceEventTimestampIsUnavailable() {
        String noTimestamp = record("bucket", "logs/a.csv", 1)
                .replace("\"eventTime\":\"2026-01-01T00:00:00.000Z\",", "");
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(message("missing-time", noTimestamp)));
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        provider.updateOffset(nextOffset());
        provider.onTaskCommitted(1, 1);
        Assertions.assertEquals(0L, provider.getLastSourceEventTimestampSeconds());
    }

    @Test
    public void testFiltersRecordsAndDeletesOnlySkippedMessages() {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(message("keep", record("bucket", "logs/a+b%2Bc.csv", 1),
                                record("other-bucket", "logs/wrong.csv", 1),
                                record("bucket", "other/wrong.csv", 1)),
                        Message.builder().receiptHandle("test-event")
                                .body("{\"Event\":\"s3:TestEvent\"}").build()),
                        ReceiveMessageResponse.builder().build());

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        Assertions.assertEquals(Collections.singletonList("logs/a b+c.csv"), nextOffset().getFiles());
        ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(sqsClient).deleteMessageBatch(deleted.capture());
        Assertions.assertEquals(Collections.singletonList("test-event"), deleteHandles(deleted.getValue()));
    }

    @Test
    public void testDuplicateKeysAreScannedOnceAndAllReceiptsAreCommitted() {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("first", "logs/a.csv", "logs/a.csv")),
                        response(filesMessage("second", "logs/a.csv", "logs/b.csv")));
        provider.fetchRemoteMeta(jobProperties(2), tvfProperties);
        S3EventOffset offset = nextOffset();
        Assertions.assertEquals(Arrays.asList("logs/a.csv", "logs/b.csv"), offset.getFiles());
        Assertions.assertEquals(2, offset.getFileStatuses().size());
        Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));
        provider.updateOffset(provider.deserializeOffset(provider.getCommitOffsetJson(offset, 1L,
                Collections.emptyList())));
        provider.onTaskCommitted(2, 2);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().build());
        provider.fetchRemoteMeta(jobProperties(2), tvfProperties);
        ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(sqsClient).deleteMessageBatch(deleted.capture());
        Assertions.assertEquals(Arrays.asList("first", "second"), deleteHandles(deleted.getValue()));
    }

    @Test
    public void testFileLimitAcceptsWholeReceive() {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(message("first", record("bucket", "logs/a.csv", 1)),
                        message("overflow", record("bucket", "logs/b.csv", 1),
                                record("bucket", "logs/c.csv", 1))));

        provider.fetchRemoteMeta(jobProperties(2), tvfProperties);

        Assertions.assertEquals(Arrays.asList("logs/a.csv", "logs/b.csv", "logs/c.csv"), nextOffset().getFiles());
        Mockito.verify(sqsClient, Mockito.never())
                .changeMessageVisibilityBatch(Mockito.any(ChangeMessageVisibilityBatchRequest.class));
        Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));
    }

    @Test
    public void testByteLimitAcceptsWholeReceive() {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.max_batch_bytes", "104857600");
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(message("first", record("bucket", "logs/a.csv", 104857600)),
                        message("overflow", record("bucket", "logs/b.csv", 1))));

        provider.fetchRemoteMeta(new StreamingJobProperties(properties), tvfProperties);

        Assertions.assertEquals(Arrays.asList("logs/a.csv", "logs/b.csv"), nextOffset().getFiles());
        Mockito.verify(sqsClient, Mockito.never())
                .changeMessageVisibilityBatch(Mockito.any(ChangeMessageVisibilityBatchRequest.class));
        Mockito.verify(sqsClient).receiveMessage(Mockito.any(ReceiveMessageRequest.class));
    }

    @Test
    public void testSerializedLimitAcceptsWholeReceive() {
        // Each key is below S3's key limit; each message is about 36 KiB of selected keys.
        String[] firstKeys = longKeys("a", 40);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("first", firstKeys),
                        filesMessage("overflow", longKeys("b", 40))));

        provider.fetchRemoteMeta(jobProperties(256), tvfProperties);

        S3EventOffset offset = nextOffset();
        List<String> expected = new ArrayList<>(Arrays.asList(firstKeys));
        expected.addAll(Arrays.asList(longKeys("b", 40)));
        Assertions.assertEquals(expected, offset.getFiles());
        Assertions.assertTrue(offset.serializedSize() > 64 * 1024);
        Mockito.verify(sqsClient, Mockito.never())
                .changeMessageVisibilityBatch(Mockito.any(ChangeMessageVisibilityBatchRequest.class));
    }

    @Test
    public void testOversizedReceiveIsAcceptedWhole() {
        String[] keys = longKeys("a", 80);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("large", keys),
                        message("overflow", record("bucket", "logs/next.csv", 1))));

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        S3EventOffset offset = nextOffset();
        List<String> expected = new ArrayList<>(Arrays.asList(keys));
        expected.add("logs/next.csv");
        Assertions.assertEquals(expected, offset.getFiles());
        Assertions.assertTrue(offset.serializedSize() > 64 * 1024);
        Mockito.verify(sqsClient, Mockito.never())
                .changeMessageVisibilityBatch(Mockito.any(ChangeMessageVisibilityBatchRequest.class));
    }

    @Test
    public void testUnmatchedMessageIsDeletedWithoutAdvancingOffset() {
        provider.restoreFromPersistInfo("{\"files\":[\"logs/committed.csv\"]}");
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("unmatched", "other/ignored.csv")),
                        ReceiveMessageResponse.builder().build());

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        Assertions.assertFalse(provider.hasMoreDataToConsume());
        Assertions.assertEquals("{\"files\":[\"logs/committed.csv\"]}", provider.getPersistInfo());
        ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(sqsClient).deleteMessageBatch(deleted.capture());
        Assertions.assertEquals(Collections.singletonList("unmatched"), deleteHandles(deleted.getValue()));
    }

    @Test
    public void testInvalidMessagesAreDeletedWithoutPublishingBatch() {
        String validEvent = message("invalid", record("bucket", "logs/a.csv", 1)).body();
        String unsupportedEvent = validEvent.replace("ObjectCreated:Put", "ObjectRemoved:Delete");
        for (String body : Arrays.asList("", " ", "not-json", "null", "[]", "{}", "{\"Records\":\"invalid\"}",
                "{\"Records\":[null]}", "{\"Event\":\"s3:TestEvent\"}",
                validEvent + " {}", validEvent + " garbage", unsupportedEvent)) {
            Mockito.clearInvocations(sqsClient);
            Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                    .thenReturn(response(Message.builder().receiptHandle("invalid").body(body).build()),
                            ReceiveMessageResponse.builder().build());

            provider.fetchRemoteMeta(jobProperties(2), tvfProperties);

            Assertions.assertFalse(provider.hasMoreDataToConsume());
            Assertions.assertNull(provider.getPersistInfo());
            ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
            Mockito.verify(sqsClient).deleteMessageBatch(deleted.capture());
            Assertions.assertEquals(Collections.singletonList("invalid"), deleteHandles(deleted.getValue()));
        }
    }

    @Test
    public void testInvalidRecordsDoNotDiscardValidFilesAndDeletionWaitsForCommit() {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(message("mixed", record("bucket", "logs/a.csv", 1), "null",
                        record("bucket", "logs/deleted.csv", 1).replace("ObjectCreated:Put", "ObjectRemoved:Delete"),
                        "{\"eventSource\":\"aws:s3\",\"eventName\":\"ObjectCreated:Put\"}",
                        record("bucket", "", 1), record("bucket", "logs/invalid.csv", -1),
                        record("bucket", "logs/invalid%ZZ.csv", 1))),
                        ReceiveMessageResponse.builder().build());

        provider.fetchRemoteMeta(jobProperties(2), tvfProperties);

        S3EventOffset offset = nextOffset();
        Assertions.assertEquals(Collections.singletonList("logs/a.csv"), offset.getFiles());
        Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));
        provider.updateOffset(offset);
        provider.onTaskCommitted(1, 1);
        Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));
        provider.fetchRemoteMeta(jobProperties(2), tvfProperties);
        ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(sqsClient).deleteMessageBatch(deleted.capture());
        Assertions.assertEquals(Collections.singletonList("mixed"), deleteHandles(deleted.getValue()));
    }

    @Test
    public void testCommitDefersDeletionAndPartialFailureBlocksNextReceive() {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(message("a", record("bucket", "logs/a.csv", 1)),
                        message("b", record("bucket", "logs/b.csv", 1))),
                        ReceiveMessageResponse.builder().build());
        provider.fetchRemoteMeta(jobProperties(2), tvfProperties);
        S3EventOffset offset = nextOffset();
        Assertions.assertSame(offset, nextOffset());
        Assertions.assertNull(provider.getPersistInfo());
        Mockito.clearInvocations(sqsClient);

        String committedJson = provider.getCommitOffsetJson(offset, 1L, Collections.emptyList());
        provider.updateOffset(provider.deserializeOffset(committedJson));
        provider.onTaskCommitted(2, 2);
        Mockito.verifyNoInteractions(sqsClient);
        Assertions.assertFalse(provider.hasMoreDataToConsume());
        Assertions.assertEquals("{\"files\":[\"logs/a.csv\",\"logs/b.csv\"]}", provider.getPersistInfo());

        Mockito.when(sqsClient.deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().failed(BatchResultErrorEntry.builder()
                        .id("1").code("RequestThrottled").senderFault(false).build()).build(),
                        DeleteMessageBatchResponse.builder().build());
        Assertions.assertThrows(IllegalStateException.class,
                () -> provider.fetchRemoteMeta(jobProperties(2), tvfProperties));
        Mockito.verify(sqsClient, Mockito.never()).receiveMessage(Mockito.any(ReceiveMessageRequest.class));
        Assertions.assertFalse(provider.hasMoreDataToConsume());

        provider.fetchRemoteMeta(jobProperties(2), tvfProperties);
        ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        InOrder order = Mockito.inOrder(sqsClient);
        order.verify(sqsClient, Mockito.times(2)).deleteMessageBatch(deleted.capture());
        order.verify(sqsClient).receiveMessage(Mockito.any(ReceiveMessageRequest.class));
        Assertions.assertEquals(Arrays.asList("a", "b"), deleteHandles(deleted.getAllValues().get(0)));
        Assertions.assertEquals(Collections.singletonList("b"), deleteHandles(deleted.getAllValues().get(1)));
    }

    @Test
    public void testDeleteRequestExceptionRetainsUnattemptedChunks() {
        Message[] first = IntStream.range(0, 10)
                .mapToObj(i -> filesMessage("h" + i, "logs/" + i + ".csv")).toArray(Message[]::new);
        Message[] second = IntStream.range(10, 13)
                .mapToObj(i -> filesMessage("h" + i, "logs/" + i + ".csv")).toArray(Message[]::new);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(first), response(second), ReceiveMessageResponse.builder().build());
        provider.fetchRemoteMeta(jobProperties(13), tvfProperties);
        Assertions.assertEquals(13, nextOffset().getFiles().size());
        provider.updateOffset(nextOffset());
        provider.onTaskCommitted(13, 13);
        Mockito.clearInvocations(sqsClient);
        RuntimeException failure = new IllegalStateException("connection failed");
        Mockito.when(sqsClient.deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class)))
                .thenThrow(failure).thenReturn(DeleteMessageBatchResponse.builder().build());

        IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class,
                () -> provider.fetchRemoteMeta(jobProperties(13), tvfProperties));
        Assertions.assertSame(failure, thrown.getCause());
        Mockito.verify(sqsClient, Mockito.never()).receiveMessage(Mockito.any(ReceiveMessageRequest.class));

        provider.fetchRemoteMeta(jobProperties(13), tvfProperties);
        ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(sqsClient, Mockito.times(3)).deleteMessageBatch(deleted.capture());
        Assertions.assertEquals(10, deleted.getAllValues().get(0).entries().size());
        Assertions.assertEquals(deleteHandles(deleted.getAllValues().get(0)),
                deleteHandles(deleted.getAllValues().get(1)));
        Assertions.assertEquals(Arrays.asList("h10", "h11", "h12"), deleteHandles(deleted.getAllValues().get(2)));
    }

    @Test
    public void testCommitCanProceedDuringRenewalAndExpiredReceiptIsNotDeleted() throws Exception {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("old", "logs/a.csv")), ReceiveMessageResponse.builder().build());
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        S3EventOffset offset = nextOffset();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Mockito.when(sqsClient.changeMessageVisibilityBatch(Mockito.any(ChangeMessageVisibilityBatchRequest.class)))
                    .thenAnswer(invocation -> {
                        ChangeMessageVisibilityBatchRequest request = invocation.getArgument(0);
                        Assertions.assertTrue(request.entries().get(0).visibilityTimeout() > 10);
                        // Another thread must finish the commit while this SQS call is still in progress.
                        executor.submit(() -> {
                            Assertions.assertEquals("{\"files\":[\"logs/a.csv\"]}",
                                    provider.getCommitOffsetJson(offset, 1L, Collections.emptyList()));
                            provider.updateOffset(offset);
                            provider.onTaskCommitted(1, 1);
                        }).get(5, TimeUnit.SECONDS);
                        return ChangeMessageVisibilityBatchResponse.builder().failed(BatchResultErrorEntry.builder()
                                .id("0").code("ReceiptHandleIsInvalid").senderFault(true).build()).build();
                    });
            provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
            Assertions.assertFalse(provider.hasMoreDataToConsume());
            provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
            Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));
            Mockito.verify(sqsClient, Mockito.times(2)).receiveMessage(Mockito.any(ReceiveMessageRequest.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidVisibilityDurationDoesNotDiscardReceipt() {
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("running", "logs/a.csv")));
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        S3EventOffset offset = nextOffset();
        Mockito.when(sqsClient.changeMessageVisibilityBatch(Mockito.any(ChangeMessageVisibilityBatchRequest.class)))
                .thenReturn(ChangeMessageVisibilityBatchResponse.builder().failed(BatchResultErrorEntry.builder()
                        .id("0").code("InvalidParameterValue").message("Visibility timeout exceeds the maximum")
                        .senderFault(true).build()).build());

        Assertions.assertThrows(IllegalStateException.class,
                () -> provider.fetchRemoteMeta(jobProperties(1), tvfProperties));
        Assertions.assertThrows(IllegalStateException.class,
                () -> provider.fetchRemoteMeta(jobProperties(1), tvfProperties));
        Assertions.assertSame(offset, nextOffset());
        Mockito.verify(sqsClient, Mockito.times(2))
                .changeMessageVisibilityBatch(Mockito.any(ChangeMessageVisibilityBatchRequest.class));
        Mockito.verify(sqsClient).receiveMessage(Mockito.any(ReceiveMessageRequest.class));
    }

    @Test
    public void testRecoveredFilesSurviveLaterCommitsAndResume() throws Exception {
        StreamingInsertJob job = Mockito.mock(StreamingInsertJob.class);
        provider.restoreFromPersistInfo("{\"files\":[\"logs/a.csv\"]}");
        provider.replayIfNeed(job);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("b", "logs/b.csv")),
                        response(filesMessage("c", "logs/c.csv")),
                        response(filesMessage("late-a", "logs/a.csv")),
                        ReceiveMessageResponse.builder().build());
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        provider.updateOffset(nextOffset());
        provider.onTaskCommitted(1, 1);
        provider.replayIfNeed(job);
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        provider.updateOffset(nextOffset());
        provider.onTaskCommitted(1, 1);
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        Assertions.assertFalse(provider.hasMoreDataToConsume());
        Assertions.assertEquals("{\"files\":[\"logs/c.csv\"]}", provider.getPersistInfo());
        Assertions.assertEquals("{\"fileName\":\"logs/c.csv\"}", provider.getShowCurrentOffset());
        ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(sqsClient, Mockito.times(3)).deleteMessageBatch(deleted.capture());
        Assertions.assertEquals(Collections.singletonList("late-a"), deleteHandles(deleted.getAllValues().get(2)));

        provider.replayIfNeed(job);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("overwrite-a", "logs/a.csv")));
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Assertions.assertEquals(Collections.singletonList("logs/a.csv"), nextOffset().getFiles());
    }

    @Test
    public void testRecoverySnapshotUsesLatestReplayedOffsetAndDoesNotAccumulate() {
        provider.restoreFromPersistInfo("{\"files\":[\"logs/image.csv\"]}");
        // Transaction replay advances the image offset before the first scheduling pass.
        provider.updateOffset(new S3EventOffset(Collections.singletonList("logs/a.csv")));
        provider.replayIfNeed(Mockito.mock(StreamingInsertJob.class));
        provider.updateOffset(new S3EventOffset(Collections.singletonList("logs/b.csv")));
        provider.replayIfNeed(Mockito.mock(StreamingInsertJob.class));
        provider.updateOffset(new S3EventOffset(Collections.singletonList("logs/c.csv")));
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("mixed", "logs/image.csv", "logs/a.csv",
                        "logs/b.csv", "logs/c.csv", "logs/new.csv")));

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        Assertions.assertEquals(Arrays.asList("logs/image.csv", "logs/b.csv", "logs/new.csv"),
                nextOffset().getFiles());
        Assertions.assertEquals(3, nextOffset().getFileStatuses().size());
        Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));
        Map<String, String> recovered = Deencapsulation.getField(provider, "recoveredCommittedFiles");
        Assertions.assertEquals(Collections.singletonMap("logs/a.csv", "mixed"), recovered);
        provider.updateOffset(nextOffset());
        provider.onTaskCommitted(3, 3);
        Assertions.assertEquals(Collections.singletonMap("logs/a.csv", "mixed"), recovered);
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().build());
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(sqsClient).deleteMessageBatch(deleted.capture());
        Assertions.assertEquals(Collections.singletonList("mixed"), deleteHandles(deleted.getValue()));
        Assertions.assertTrue(recovered.isEmpty());
    }

    @Test
    public void testRecoveredFilesAreRemovedOnlyAfterSuccessfulDeletion() {
        provider.restoreFromPersistInfo("{\"files\":[\"logs/a.csv\",\"logs/b.csv\",\"logs/unseen.csv\"]}");
        provider.replayIfNeed(Mockito.mock(StreamingInsertJob.class));
        provider.updateOffset(new S3EventOffset(Collections.singletonList("logs/current.csv")));
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("a", "logs/a.csv"), filesMessage("b", "logs/b.csv")));
        Mockito.when(sqsClient.deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder()
                        .successful(DeleteMessageBatchResultEntry.builder().id("0").build())
                        .failed(BatchResultErrorEntry.builder().id("1").code("RequestThrottled").build()).build());

        Assertions.assertThrows(IllegalStateException.class,
                () -> provider.fetchRemoteMeta(jobProperties(1), tvfProperties));
        Map<String, String> recovered = Deencapsulation.getField(provider, "recoveredCommittedFiles");
        Assertions.assertFalse(recovered.containsKey("logs/a.csv"));
        Assertions.assertEquals("b", recovered.get("logs/b.csv"));
        Assertions.assertTrue(recovered.containsKey("logs/unseen.csv"));

        Mockito.when(sqsClient.deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class)))
                .thenThrow(new IllegalStateException("delete failed"))
                .thenReturn(DeleteMessageBatchResponse.builder()
                        .successful(DeleteMessageBatchResultEntry.builder().id("0").build()).build());
        Assertions.assertThrows(IllegalStateException.class,
                () -> provider.fetchRemoteMeta(jobProperties(1), tvfProperties));
        Assertions.assertEquals("b", recovered.get("logs/b.csv"));
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().build());
        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);
        Assertions.assertFalse(recovered.containsKey("logs/b.csv"));
        Assertions.assertEquals(Collections.singleton("logs/unseen.csv"), recovered.keySet());
        Assertions.assertEquals("{\"files\":[\"logs/current.csv\"]}", provider.getPersistInfo());
    }

    @Test
    public void testFreshJobDoesNotCaptureLaterCommitAsRecoverySnapshot() {
        StreamingInsertJob job = Mockito.mock(StreamingInsertJob.class);
        provider.replayIfNeed(job);
        provider.updateOffset(new S3EventOffset(Collections.singletonList("logs/a.csv")));
        provider.replayIfNeed(job);
        provider.updateOffset(new S3EventOffset(Collections.singletonList("logs/b.csv")));
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("old", "logs/a.csv")));

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        Assertions.assertEquals(Collections.singletonList("logs/a.csv"), nextOffset().getFiles());
    }

    @Test
    public void testRestoreKeepsCommittedFilesButNotReceiptHandles() {
        provider.restoreFromPersistInfo("{\"files\":[\"logs/committed.csv\"]}");
        StreamingInsertJob job = Mockito.mock(StreamingInsertJob.class);
        Mockito.when(job.getOffsetProviderPersist()).thenReturn("{\"files\":[\"logs/older.csv\"]}");
        provider.replayIfNeed(job);
        Assertions.assertFalse(provider.hasMoreDataToConsume());
        Assertions.assertEquals("{\"files\":[\"logs/committed.csv\"]}", provider.getPersistInfo());
        Assertions.assertEquals("{\"fileName\":\"logs/committed.csv\"}", provider.getShowCurrentOffset());
        Assertions.assertEquals("{\"fileName\":\"logs/committed.csv\"}", provider.getShowMaxOffset());
        Mockito.when(sqsClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class)))
                .thenReturn(response(filesMessage("new-handle", "logs/committed.csv"),
                        filesMessage("next", "logs/next.csv")));

        provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

        Assertions.assertEquals(Collections.singletonList("logs/next.csv"), nextOffset().getFiles());
        ArgumentCaptor<DeleteMessageBatchRequest> deleted = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        InOrder order = Mockito.inOrder(sqsClient);
        order.verify(sqsClient).receiveMessage(Mockito.any(ReceiveMessageRequest.class));
        order.verify(sqsClient).deleteMessageBatch(deleted.capture());
        Assertions.assertEquals(Collections.singletonList("new-handle"), deleteHandles(deleted.getValue()));
    }

    @Test
    public void testSqsUsesIamRoleWithoutStaticKeys() throws Exception {
        tvfProperties.remove("s3.access_key");
        tvfProperties.remove("s3.secret_key");
        tvfProperties.put("s3.role_arn", "arn:aws:iam::123456789012:role/event-reader");
        tvfProperties.put("s3.external_id", "event-external-id");
        tvfProperties.put("s3.credentials_provider_type", "instance_profile");
        StsClient stsClient = Mockito.mock(StsClient.class);
        StsClientBuilder stsBuilder = Mockito.mock(StsClientBuilder.class, Mockito.RETURNS_SELF);
        Mockito.when(stsBuilder.build()).thenReturn(stsClient);
        Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
                .thenReturn(AssumeRoleResponse.builder().credentials(Credentials.builder()
                        .accessKeyId("role-ak").secretAccessKey("role-sk").sessionToken("role-token")
                        .expiration(Instant.now().plusSeconds(3600)).build()).build());
        try (MockedStatic<StsClient> stsStatic = Mockito.mockStatic(StsClient.class)) {
            stsStatic.when(StsClient::builder).thenReturn(stsBuilder);
            provider = new S3EventSourceOffsetProvider(QUEUE_URL);
            provider.ensureInitialized(1L, tvfProperties);
            provider.fetchRemoteMeta(jobProperties(1), tvfProperties);

            ArgumentCaptor<AwsCredentialsProvider> credentials = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
            Mockito.verify(sqsBuilder).credentialsProvider(
                    (IdentityProvider<? extends AwsCredentialsIdentity>) credentials.capture());
            // Keep the real role provider: only STS and SQS network boundaries are mocked.
            try (StsAssumeRoleCredentialsProvider roleProvider =
                    (StsAssumeRoleCredentialsProvider) credentials.getValue()) {
                AwsSessionCredentials resolved = (AwsSessionCredentials) roleProvider.resolveCredentials();
                Assertions.assertEquals("role-ak", resolved.accessKeyId());
                Assertions.assertEquals("role-sk", resolved.secretAccessKey());
                Assertions.assertEquals("role-token", resolved.sessionToken());
            }
            ArgumentCaptor<AssumeRoleRequest> assumed = ArgumentCaptor.forClass(AssumeRoleRequest.class);
            Mockito.verify(stsClient).assumeRole(assumed.capture());
            Assertions.assertEquals("arn:aws:iam::123456789012:role/event-reader", assumed.getValue().roleArn());
            Assertions.assertEquals("event-external-id", assumed.getValue().externalId());
            Mockito.verify(sqsBuilder).region(Region.US_WEST_2);
            Mockito.verify(sqsClient, Mockito.never()).close();
        }
    }

    private StreamingJobProperties jobProperties(int maxFiles) {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.max_batch_files", String.valueOf(maxFiles));
        return new StreamingJobProperties(properties);
    }

    private S3EventOffset nextOffset() {
        return provider.getNextOffset(jobProperties(256), tvfProperties);
    }

    private static List<String> deleteHandles(DeleteMessageBatchRequest request) {
        Assertions.assertEquals(QUEUE_URL, request.queueUrl());
        return request.entries().stream().map(entry -> entry.receiptHandle()).collect(Collectors.toList());
    }

    private static ReceiveMessageResponse response(Message... messages) {
        return ReceiveMessageResponse.builder().messages(messages).build();
    }

    private static Message message(String handle, String... records) {
        return Message.builder().messageId("id-" + handle).receiptHandle(handle)
                .body("{\"Records\":[" + String.join(",", records) + "]}").build();
    }

    private static Message filesMessage(String handle, String... keys) {
        return message(handle, Arrays.stream(keys).map(key -> record("bucket", key, 1)).toArray(String[]::new));
    }

    private static String[] longKeys(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(i -> "logs/" + prefix + i + StringUtils.repeat('x', 900) + ".csv")
                .toArray(String[]::new);
    }

    private static String record(String bucket, String encodedKey, long size) {
        return record(bucket, encodedKey, size, "2026-01-01T00:00:00.000Z");
    }

    private static String record(String bucket, String encodedKey, long size, String eventTime) {
        return "{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-west-2\","
                + "\"eventTime\":" + GsonUtils.GSON.toJson(eventTime) + ",\"eventName\":\"ObjectCreated:Put\","
                + "\"userIdentity\":{\"principalId\":\"test\"},"
                + "\"requestParameters\":{\"sourceIPAddress\":\"127.0.0.1\"},"
                + "\"responseElements\":{\"x-amz-request-id\":\"request\",\"x-amz-id-2\":\"host\"},"
                + "\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"test\","
                + "\"bucket\":{\"name\":" + GsonUtils.GSON.toJson(bucket)
                + ",\"ownerIdentity\":{\"principalId\":\"owner\"},\"arn\":\"arn:aws:s3:::" + bucket + "\"},"
                + "\"object\":{\"key\":" + GsonUtils.GSON.toJson(encodedKey) + ",\"size\":" + size
                + ",\"eTag\":\"etag\",\"sequencer\":\"001\"}}}";
    }
}
