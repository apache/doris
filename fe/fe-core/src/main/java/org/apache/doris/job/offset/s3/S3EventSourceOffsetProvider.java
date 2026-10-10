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

import org.apache.doris.common.util.S3URI;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.S3CompatibleFileSystem;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Consumes S3 object-created notifications from SQS for streaming INSERT jobs.
 */
@Log4j2
public class S3EventSourceOffsetProvider implements SourceOffsetProvider {
    private static final int SQS_BATCH_SIZE = 10;
    private static final int MIN_VISIBILITY_SECONDS = 120;
    private static final int VISIBILITY_INTERVAL_MULTIPLIER = 6;

    private final String queueUrl;
    private transient Long jobId;
    private transient String sourceBucket;
    private transient Pattern sourceKeyMatcher;
    // Only one task batch is retained, either ready for scheduling or assigned to a task.
    private transient TaskBatch readyBatch;
    private transient TaskBatch runningBatch;
    // Includes both committed task messages and skipped messages whose deletion must be retried.
    private transient List<String> receiptsToDelete = new ArrayList<>();
    private transient S3EventOffset currentOffset;
    // Recovered keys map to their latest receipts and are removed after successful SQS deletion.
    private transient Map<String, String> recoveredCommittedFiles;
    // Display-only statistic; unavailable after restart until another batch commits.
    private transient long lastSourceEventTimestampSeconds;
    // Approximate visible backlog only; null means the queue statistic is unavailable.
    private transient Long approximateNumberOfMessages;
    private transient volatile boolean closed;
    // S3 and SQS share a refreshable credentials provider, which must stay open until SQS closes.
    private transient FileSystem fileSystem;
    private transient SqsClient sqsClient;
    private transient Map<String, String> clientProperties;

    public S3EventSourceOffsetProvider(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    @Override
    public String getSourceType() {
        return "s3";
    }

    @Override
    public synchronized void ensureInitialized(Long jobId, Map<String, String> originTvfProps) throws JobException {
        try {
            this.jobId = jobId;
            Map<String, String> copiedProps = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            copiedProps.putAll(originTvfProps);
            StorageAdapter storageProperties = StorageAdapter.of(copiedProps);
            if (!"S3".equals(storageProperties.getSpiProperties().providerName())) {
                throw new JobException("S3 NOTIFICATION mode currently supports AWS S3 only");
            }
            S3CompatibleFileSystemProperties s3Properties =
                    (S3CompatibleFileSystemProperties) storageProperties.getSpiProperties();
            String uri = storageProperties.validateAndGetUri(copiedProps);
            S3URI s3Uri = S3URI.create(storageProperties.validateAndNormalizeUri(uri));
            sourceBucket = s3Uri.getBucket();
            if (sourceKeyMatcher == null) {
                sourceKeyMatcher = S3CompatibleFileSystem.compileGlobPattern(s3Uri.getKey());
            }

            if (StringUtils.isBlank(s3Properties.getRegion())) {
                throw new JobException("s3.region is required for S3 NOTIFICATION mode");
            }
        } catch (JobException e) {
            throw e;
        } catch (Exception e) {
            throw new JobException("Failed to initialize S3 EVENT source: " + e.getMessage());
        }
    }

    @Override
    public synchronized S3EventOffset getNextOffset(
            StreamingJobProperties jobProperties, Map<String, String> properties) {
        if (runningBatch != null) {
            return runningBatch.offset;
        }
        Preconditions.checkState(readyBatch != null, "No ready S3 event batch");
        runningBatch = readyBatch;
        readyBatch = null;
        return runningBatch.offset;
    }

    @Override
    public synchronized String getShowCurrentOffset() {
        String fileName = lastFileName(currentOffset);
        return fileName == null ? null : GsonUtils.GSON.toJson(Collections.singletonMap("fileName", fileName));
    }

    @Override
    public synchronized long getLastSourceEventTimestampSeconds() {
        return lastSourceEventTimestampSeconds;
    }

    @Override
    public synchronized long getLagMessages() {
        return approximateNumberOfMessages == null ? -1 : approximateNumberOfMessages;
    }

    @Override
    public synchronized String getShowMaxOffset() {
        // Only accepted files count as observed progress; overflow and skipped messages do not.
        S3EventOffset observedOffset = readyBatch != null ? readyBatch.offset
                : runningBatch != null ? runningBatch.offset : currentOffset;
        String fileName = lastFileName(observedOffset);
        JsonObject result = new JsonObject();
        if (fileName != null) {
            result.addProperty("fileName", fileName);
        }
        if (approximateNumberOfMessages != null) {
            result.addProperty("lagMessages", approximateNumberOfMessages);
        }
        return result.size() == 0 ? null : GsonUtils.GSON.toJson(result);
    }

    private String lastFileName(S3EventOffset offset) {
        if (offset == null || offset.isEmpty()) {
            return null;
        }
        // Receive order is not lexical order; this is a display value, not a recovery cursor.
        List<String> files = offset.getFiles();
        return files.get(files.size() - 1);
    }

    @Override
    public InsertIntoTableCommand rewriteTvfParams(
            InsertIntoTableCommand originCommand, Offset runningOffset, long taskId) {
        S3EventOffset offset = (S3EventOffset) runningOffset;
        return S3SourceOffsetProvider.rewriteS3Tvf(originCommand, offset.getFileStatuses());
    }

    @Override
    public synchronized void updateOffset(Offset offset) {
        currentOffset = (S3EventOffset) offset;
    }

    @Override
    public void close() {
        FileSystem fileSystemToClose;
        SqsClient sqsClientToClose;
        synchronized (this) {
            closed = true;
            fileSystemToClose = fileSystem;
            sqsClientToClose = sqsClient;
            fileSystem = null;
            sqsClient = null;
            clientProperties = null;
        }
        closeClientResources(fileSystemToClose, sqsClientToClose);
    }

    private void closeClientResources(FileSystem fileSystemToClose, SqsClient sqsClientToClose) {
        if (sqsClientToClose != null) {
            try {
                sqsClientToClose.close();
            } catch (RuntimeException e) {
                log.warn("Failed to close SQS client for job {}", jobId, e);
            }
        }
        if (fileSystemToClose != null) {
            try {
                fileSystemToClose.close();
            } catch (IOException | RuntimeException e) {
                log.warn("Failed to close S3 filesystem for job {}", jobId, e);
            }
        }
    }

    @Override
    public void fetchRemoteMeta(
            StreamingJobProperties jobProperties, Map<String, String> properties) {
        // The job scheduler serializes fetches; task callbacks still access batch state concurrently.
        TaskBatch batch = null;
        try {
            SqsClient client;
            List<String> receipts;
            synchronized (this) {
                if (closed) {
                    return;
                }
                Map<String, String> copiedProps = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                copiedProps.putAll(properties);
                // A pre-pause fetch may have old properties after ALTER/RESUME.
                if (sqsClient != null && !copiedProps.equals(clientProperties)) {
                    closeClientResources(fileSystem, sqsClient);
                    fileSystem = null;
                    sqsClient = null;
                    clientProperties = null;
                }
                if (sqsClient == null) {
                    try {
                        fileSystem = FileSystemFactory.getFileSystem(StorageAdapter.of(copiedProps));
                        S3Client s3Client = (S3Client) ((ObjFileSystem) fileSystem).getObjStorage().getClient();
                        sqsClient = createSqsClient(s3Client);
                        clientProperties = copiedProps;
                    } catch (IOException | RuntimeException e) {
                        closeClientResources(fileSystem, sqsClient);
                        fileSystem = null;
                        sqsClient = null;
                        throw e;
                    }
                }
                client = sqsClient;
                batch = readyBatch != null ? readyBatch : runningBatch;
                receipts = new ArrayList<>(batch == null ? receiptsToDelete : batch.receiptHandles);
            }
            int visibilityTimeoutSeconds = (int) Math.max(MIN_VISIBILITY_SECONDS,
                    VISIBILITY_INTERVAL_MULTIPLIER * jobProperties.getMaxIntervalSecond());
            try {
                if (batch != null) {
                    // Invalid handles do not invalidate the batch; still refresh its queue statistic.
                    if (!receipts.isEmpty()) {
                        renewBatchVisibility(client, batch, receipts, visibilityTimeoutSeconds);
                    }
                    return;
                }
                if (!receipts.isEmpty()) {
                    deletePendingMessages(client, receipts);
                }
                receiveBatch(client, jobProperties, visibilityTimeoutSeconds);
            } finally {
                if (!closed) {
                    refreshApproximateNumberOfMessages(client);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to access the S3 EVENT source", e);
        } catch (RuntimeException e) {
            synchronized (this) {
                if (closed) {
                    return;
                }
                if (batch != null && batch != readyBatch && batch != runningBatch) {
                    log.warn("SQS visibility update failed after the S3 EVENT batch committed", e);
                    return;
                }
            }
            throw e;
        }
    }

    private void refreshApproximateNumberOfMessages(SqsClient sqsClient) {
        Long count = null;
        try {
            String value = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                    .build()).attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
            count = Long.parseLong(value);
        } catch (RuntimeException e) {
            // Monitoring must not block ingestion or report a stale count as current backlog.
            if (!closed) {
                log.warn("Failed to fetch SQS pending message count for queue {}", queueUrl, e);
            }
        }
        synchronized (this) {
            if (!closed) {
                approximateNumberOfMessages = count;
            }
        }
    }

    private SqsClient createSqsClient(S3Client s3Client) {
        return SqsClient.builder()
                .region(s3Client.serviceClientConfiguration().region())
                .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
                .build();
    }

    @Override
    public void fetchRemoteMeta(Map<String, String> properties) {
        throw new UnsupportedOperationException("S3 EVENT metadata fetch requires streaming job properties");
    }

    private void renewBatchVisibility(
            SqsClient sqsClient, TaskBatch batch, List<String> receipts, int visibilityTimeoutSeconds) {
        boolean renewed = true;
        Set<String> invalidReceipts = new HashSet<>();
        try {
            for (int start = 0; start < receipts.size(); start += SQS_BATCH_SIZE) {
                List<String> chunk = receipts.subList(start, Math.min(start + SQS_BATCH_SIZE, receipts.size()));
                List<BatchResultErrorEntry> failures =
                        changeMessageVisibility(sqsClient, chunk, visibilityTimeoutSeconds);
                for (BatchResultErrorEntry failure : failures) {
                    if (isInvalidReceipt(failure)) {
                        invalidReceipts.add(chunk.get(Integer.parseInt(failure.id())));
                    } else {
                        renewed = false;
                    }
                }
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to renew visibility for the current S3 EVENT batch", e);
        } finally {
            if (!invalidReceipts.isEmpty()) {
                synchronized (this) {
                    // Receipt expiry does not invalidate the task's fixed file list.
                    batch.receiptHandles.removeAll(invalidReceipts);
                    // The task may have committed and copied these handles while the RPC was in progress.
                    receiptsToDelete.removeAll(invalidReceipts);
                }
            }
        }
        if (!renewed) {
            throw new IllegalStateException(
                    "Failed to renew visibility for some messages in the current S3 EVENT batch");
        }
    }

    private void receiveBatch(
            SqsClient sqsClient, StreamingJobProperties jobProperties,
            int visibilityTimeoutSeconds) {
        List<String> files = new ArrayList<>();
        List<String> receipts = new ArrayList<>();
        List<TBrokerFileStatus> fileStatuses = new ArrayList<>();
        Set<String> selectedFiles = new HashSet<>();
        long fileBytes = 0;
        long serializedSize = new S3EventOffset(Collections.emptyList()).serializedSize();
        long batchEventTimestampSeconds = 0;
        int receivedMessages = 0;
        int skippedMessages = 0;
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(SQS_BATCH_SIZE)
                .waitTimeSeconds(1)
                .visibilityTimeout(visibilityTimeoutSeconds)
                .build();
        // Cap consecutive receives per fetch; this does not interrupt an ongoing SQS request.
        long maxFetchDurationNanos = Math.min(TimeUnit.SECONDS.toNanos(jobProperties.getMaxIntervalSecond()) / 3,
                TimeUnit.SECONDS.toNanos(5));
        long fetchEndTimeNanos = System.nanoTime() + maxFetchDurationNanos;

        do {
            ReceiveMessageResponse response = sqsClient.receiveMessage(request);
            if (closed) {
                return;
            }
            List<Message> messages = response.messages();
            receivedMessages += messages.size();
            if (messages.isEmpty()) {
                break;
            }

            List<ParsedMessage> candidates = parseMessages(sqsClient, messages, receipts);
            // Delete skipped messages before filtering them out or stopping at the batch limit.
            // Messages selected for INSERT are deleted only after the task commits.
            deleteSkippedMessages(sqsClient, candidates, receipts);
            candidates.removeIf(message -> message.files.isEmpty());
            skippedMessages += messages.size() - candidates.size();

            // Thresholds stop the next receive; every selected message from this receive stays in this batch.
            for (ParsedMessage candidate : candidates) {
                for (int i = 0; i < candidate.files.size(); i++) {
                    String key = candidate.files.get(i);
                    // SQS may deliver the same key more than once; scan it once but acknowledge every receipt.
                    if (selectedFiles.add(key)) {
                        TBrokerFileStatus file = candidate.fileStatuses.get(i);
                        files.add(key);
                        fileStatuses.add(file);
                        fileBytes = Math.addExact(fileBytes, file.getSize());
                        serializedSize += GsonUtils.GSON.toJson(key).getBytes(StandardCharsets.UTF_8).length + 1;
                    }
                }
                receipts.add(candidate.receiptHandle);
                batchEventTimestampSeconds = Math.max(batchEventTimestampSeconds, candidate.eventTimestampSeconds);
            }
            if (files.size() >= jobProperties.getS3BatchFiles()
                    || fileBytes >= jobProperties.getS3BatchBytes()
                    || serializedSize >= S3EventOffset.TARGET_BATCH_SERIALIZED_BYTES) {
                break;
            }
        } while (!closed && System.nanoTime() < fetchEndTimeNanos);

        if (!files.isEmpty()) {
            makeReadyBatch(files, fileStatuses, receipts, batchEventTimestampSeconds);
        }
        if (receivedMessages > 0) {
            log.info("S3 EVENT receive batch: queue={}, receivedMessages={}, skippedMessages={}, "
                            + "selectedMessages={}, selectedFiles={}",
                    queueUrl, receivedMessages, skippedMessages, receipts.size(), files.size());
        }
    }

    private List<ParsedMessage> parseMessages(
            SqsClient sqsClient, List<Message> messages, List<String> acceptedReceipts) {
        try {
            List<ParsedMessage> parsedMessages = new ArrayList<>(messages.size());
            for (Message message : messages) {
                if (StringUtils.isBlank(message.receiptHandle())) {
                    throw new IllegalArgumentException("SQS message has no receipt handle");
                }
                parsedMessages.add(parseMessage(message));
            }
            return parsedMessages;
        } catch (RuntimeException e) {
            // No batch will be published; make uncommitted messages available for retry.
            List<String> handles = new ArrayList<>(acceptedReceipts);
            handles.addAll(messages.stream().map(Message::receiptHandle)
                    .filter(StringUtils::isNotBlank).collect(Collectors.toList()));
            releaseMessages(sqsClient, handles);
            throw e;
        }
    }

    private void deleteSkippedMessages(SqsClient sqsClient, List<ParsedMessage> messages,
            List<String> acceptedReceipts) {
        List<String> skippedReceipts = messages.stream()
                .filter(message -> message.files.isEmpty())
                .map(message -> message.receiptHandle)
                .collect(Collectors.toList());
        if (skippedReceipts.isEmpty()) {
            return;
        }
        synchronized (this) {
            receiptsToDelete.addAll(skippedReceipts);
        }
        try {
            deletePendingMessages(sqsClient, skippedReceipts);
        } catch (RuntimeException e) {
            releaseMessages(sqsClient, collectReceiptsToRelease(acceptedReceipts, messages));
            throw e;
        }
    }

    private ParsedMessage parseMessage(Message message) {
        S3EventNotification notification;
        try {
            JsonNode json = Jackson.getObjectMapper().reader()
                    .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(message.body());
            if (json == null || !json.isObject()) {
                log.warn("Skip invalid S3 EVENT message: jobId={}, messageId={}, reason=Expected a JSON object",
                        jobId, message.messageId());
                return ParsedMessage.skipped(message.receiptHandle());
            }
            if ("s3:TestEvent".equals(json.path("Event").asText())) {
                return ParsedMessage.skipped(message.receiptHandle());
            }
            notification = Jackson.getObjectMapper().treeToValue(json, S3EventNotification.class);
        } catch (JsonProcessingException e) {
            // Only decoding failures are skipped; remote operation failures must retain the message.
            log.warn("Skip invalid S3 EVENT message: jobId={}, messageId={}, reason={}",
                    jobId, message.messageId(), e.getMessage());
            return ParsedMessage.skipped(message.receiptHandle());
        }
        if (notification.getRecords() == null || notification.getRecords().isEmpty()) {
            log.warn("Skip invalid S3 EVENT message: jobId={}, messageId={}, reason=No S3 event records",
                    jobId, message.messageId());
            return ParsedMessage.skipped(message.receiptHandle());
        }
        List<String> files = new ArrayList<>(notification.getRecords().size());
        List<TBrokerFileStatus> fileStatuses = new ArrayList<>();
        long eventTimestampSeconds = 0;
        for (int i = 0; i < notification.getRecords().size(); i++) {
            S3EventNotificationRecord record = notification.getRecords().get(i);
            String skipReason = null;
            if (record == null) {
                skipReason = "Null S3 event record";
            } else if (!"aws:s3".equals(record.getEventSource())
                    || !StringUtils.startsWith(record.getEventName(), "ObjectCreated:")) {
                skipReason = "Unsupported S3 event";
            } else if (record.getS3() == null || record.getS3().getBucket() == null
                    || record.getS3().getObject() == null) {
                skipReason = "Incomplete S3 event";
            } else if (StringUtils.isBlank(record.getS3().getBucket().getName())
                    || StringUtils.isBlank(record.getS3().getObject().getKey())) {
                skipReason = "Missing S3 bucket or object key";
            } else if (record.getS3().getObject().getSizeAsLong() == null
                    || record.getS3().getObject().getSizeAsLong() < 0) {
                skipReason = "Invalid S3 object size";
            }
            if (skipReason != null) {
                log.warn("Skip invalid S3 EVENT record: jobId={}, messageId={}, recordIndex={}, reason={}",
                        jobId, message.messageId(), i, skipReason);
                continue;
            }
            String bucket = record.getS3().getBucket().getName();
            String key;
            try {
                key = record.getS3().getObject().getUrlDecodedKey();
                if (!sourceBucket.equals(bucket) || !sourceKeyMatcher.matcher(key).matches()) {
                    continue;
                }
            } catch (IllegalArgumentException e) {
                log.warn("Skip invalid S3 EVENT record: jobId={}, messageId={}, recordIndex={}, reason={}",
                        jobId, message.messageId(), i, e.getMessage());
                continue;
            }
            if (isCommitted(key, message.receiptHandle())) {
                continue;
            }
            files.add(key);
            // The object must remain unchanged until ingestion completes; notifications identify keys, not versions.
            fileStatuses.add(new TBrokerFileStatus("s3://" + bucket + "/" + key, false,
                    record.getS3().getObject().getSizeAsLong(), true));
            if (record.getEventTime() != null) {
                eventTimestampSeconds = Math.max(eventTimestampSeconds, record.getEventTime().getMillis() / 1000);
            }
        }
        if (files.isEmpty()) {
            return ParsedMessage.skipped(message.receiptHandle());
        }
        return new ParsedMessage(message.receiptHandle(), files, fileStatuses, eventTimestampSeconds);
    }

    private synchronized boolean isCommitted(String file, String receiptHandle) {
        if (recoveredCommittedFiles != null && recoveredCommittedFiles.containsKey(file)) {
            recoveredCommittedFiles.put(file, receiptHandle);
            return true;
        }
        return currentOffset != null && currentOffset.getFiles().contains(file);
    }

    private synchronized void makeReadyBatch(List<String> files, List<TBrokerFileStatus> fileStatuses,
            List<String> receipts, long eventTimestampSeconds) {
        if (closed) {
            return;
        }
        Preconditions.checkState(!files.isEmpty() && !receipts.isEmpty(),
                "Cannot create an empty S3 EVENT batch");
        S3EventOffset offset = new S3EventOffset(files, fileStatuses);
        readyBatch = new TaskBatch(offset, new ArrayList<>(receipts), eventTimestampSeconds);
    }

    private List<String> collectReceiptsToRelease(
            List<String> acceptedReceipts, List<ParsedMessage> parsedMessages) {
        List<String> handles = new ArrayList<>(acceptedReceipts);
        handles.addAll(parsedMessages.stream().filter(message -> !message.files.isEmpty())
                .map(message -> message.receiptHandle).collect(Collectors.toList()));
        return handles;
    }

    private void releaseMessages(SqsClient sqsClient, List<String> handles) {
        if (handles.isEmpty()) {
            return;
        }
        try {
            for (int start = 0; start < handles.size(); start += SQS_BATCH_SIZE) {
                List<String> chunk = handles.subList(start, Math.min(start + SQS_BATCH_SIZE, handles.size()));
                changeMessageVisibility(sqsClient, chunk, 0);
            }
        } catch (RuntimeException e) {
            log.warn("Failed to release messages from a failed S3 EVENT receive batch", e);
        }
    }

    private List<BatchResultErrorEntry> changeMessageVisibility(
            SqsClient sqsClient, List<String> handles, int visibilityTimeout) {
        List<ChangeMessageVisibilityBatchRequestEntry> entries = new ArrayList<>(handles.size());
        for (int i = 0; i < handles.size(); i++) {
            entries.add(ChangeMessageVisibilityBatchRequestEntry.builder()
                    .id(String.valueOf(i))
                    .receiptHandle(handles.get(i))
                    .visibilityTimeout(visibilityTimeout)
                    .build());
        }
        ChangeMessageVisibilityBatchResponse response = sqsClient.changeMessageVisibilityBatch(
                ChangeMessageVisibilityBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(entries)
                        .build());
        logBatchFailures("ChangeMessageVisibilityBatch", response.failed());
        if (visibilityTimeout == 0) {
            log.info("S3 EVENT release messages: queue={}, requestedMessages={}, "
                            + "succeededMessages={}, failedMessages={}",
                    queueUrl, handles.size(), response.successful().size(), response.failed().size());
        }
        return response.failed();
    }

    private void deletePendingMessages(SqsClient sqsClient, List<String> pending) {
        List<String> failed = new ArrayList<>();
        for (int start = 0; start < pending.size(); start += SQS_BATCH_SIZE) {
            int end = Math.min(start + SQS_BATCH_SIZE, pending.size());
            List<String> chunk = pending.subList(start, end);
            try {
                DeleteMessageBatchResponse response = sqsClient.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(buildDeleteEntries(chunk))
                        .build());
                logBatchFailures("DeleteMessageBatch", response.failed());
                log.info("S3 EVENT delete messages: queue={}, requestedMessages={}, "
                                + "succeededMessages={}, failedMessages={}",
                        queueUrl, chunk.size(), response.successful().size(), response.failed().size());
                synchronized (this) {
                    if (recoveredCommittedFiles != null && !recoveredCommittedFiles.isEmpty()) {
                        Set<String> deletedReceipts = response.successful().stream()
                                .map(entry -> chunk.get(Integer.parseInt(entry.id())))
                                .collect(Collectors.toSet());
                        recoveredCommittedFiles.values().removeAll(deletedReceipts);
                    }
                }
                failed.addAll(getRetryableReceiptHandles(chunk, response.failed()));
            } catch (RuntimeException e) {
                // Retain current and unattempted chunks; the request outcome is unknown.
                failed.addAll(pending.subList(start, pending.size()));
                synchronized (this) {
                    receiptsToDelete = failed;
                }
                throw new IllegalStateException("Failed to delete S3 EVENT messages", e);
            }
        }
        synchronized (this) {
            // Fetch is serialized and no task batch exists during deletion.
            receiptsToDelete = failed;
        }
        if (!failed.isEmpty()) {
            throw new IllegalStateException("Failed to delete some S3 EVENT messages");
        }
    }

    private boolean isInvalidReceipt(BatchResultErrorEntry failure) {
        // InvalidParameterValue also covers invalid visibility durations, so only match explicit receipt expiry.
        return "ReceiptHandleIsInvalid".equals(failure.code())
                || "MessageNotInflight".equals(failure.code())
                || ("InvalidParameterValue".equals(failure.code())
                        && StringUtils.containsIgnoreCase(failure.message(), "receipt handle has expired"));
    }

    private void logBatchFailures(String operation, List<BatchResultErrorEntry> failures) {
        for (BatchResultErrorEntry failure : failures) {
            log.warn("SQS {} failed: id={}, code={}, senderFault={}",
                    operation, failure.id(), failure.code(), failure.senderFault());
            if (isInvalidReceipt(failure)) {
                // discard unusable receipts; durable event deduplication is outside this mode's scope.
                log.warn("SQS {} receipt is no longer usable: id={}; message deletion is not confirmed",
                        operation, failure.id());
            }
        }
    }

    private List<DeleteMessageBatchRequestEntry> buildDeleteEntries(List<String> handles) {
        List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>(handles.size());
        for (int i = 0; i < handles.size(); i++) {
            entries.add(DeleteMessageBatchRequestEntry.builder()
                    .id(String.valueOf(i))
                    .receiptHandle(handles.get(i))
                    .build());
        }
        return entries;
    }

    private List<String> getRetryableReceiptHandles(List<String> handles, List<BatchResultErrorEntry> failures) {
        return failures.stream().filter(failure -> !isInvalidReceipt(failure))
                .map(failure -> handles.get(Integer.parseInt(failure.id())))
                .collect(Collectors.toList());
    }

    @Override
    public synchronized boolean hasMoreDataToConsume() {
        return readyBatch != null || runningBatch != null;
    }

    @Override
    public synchronized String getPersistInfo() {
        return currentOffset == null ? null : currentOffset.toSerializedJson();
    }

    @Override
    public synchronized void restoreFromPersistInfo(String persistInfo) {
        if (persistInfo == null) {
            return;
        }
        try {
            currentOffset = GsonUtils.GSON.fromJson(persistInfo, S3EventOffset.class);
        } catch (Exception e) {
            log.warn("Failed to restore S3 EVENT offset from persist info", e);
        }
    }

    @Override
    public synchronized void replayIfNeed(StreamingInsertJob job) {
        if (currentOffset == null) {
            restoreFromPersistInfo(job.getOffsetProviderPersist());
        }
        // Capture after transaction replay; subsequent commits and resumes must not replace this snapshot.
        if (recoveredCommittedFiles == null) {
            recoveredCommittedFiles = new HashMap<>();
            if (currentOffset != null) {
                for (String file : currentOffset.getFiles()) {
                    recoveredCommittedFiles.put(file, null);
                }
            }
        }
    }

    @Override
    public Offset deserializeOffset(String offset) {
        return GsonUtils.GSON.fromJson(offset, S3EventOffset.class);
    }

    @Override
    public Offset deserializeOffsetProperty(String offset) {
        return null;
    }

    @Override
    public void validateAlterOffset(String offset) throws JobException {
        throw new JobException("offset cannot be altered when s3.ingestion_mode is NOTIFICATION");
    }

    @Override
    public synchronized String getCommitOffsetJson(Offset runningOffset, long taskId, List<Long> scanBackendIds) {
        Preconditions.checkState(runningBatch != null, "No running S3 EVENT batch");
        S3EventOffset offset = (S3EventOffset) runningOffset;
        Preconditions.checkState(runningBatch.offset.getFiles().equals(offset.getFiles()),
                "S3 EVENT running offset changed before commit");
        return offset.toSerializedJson();
    }

    @Override
    public synchronized void onTaskCommitted(long scannedRows, long loadBytes) {
        Preconditions.checkState(runningBatch != null, "No running S3 EVENT batch to commit");
        Preconditions.checkState(receiptsToDelete.isEmpty(), "Previous S3 EVENT message deletion is pending");
        Preconditions.checkState(currentOffset != null
                        && currentOffset.getFiles().equals(runningBatch.offset.getFiles()),
                "Committed S3 EVENT offset does not match the running batch");
        receiptsToDelete = new ArrayList<>(runningBatch.receiptHandles);
        lastSourceEventTimestampSeconds = runningBatch.eventTimestampSeconds;
        runningBatch = null;
    }

    private static class TaskBatch {
        private final S3EventOffset offset;
        private final List<String> receiptHandles;
        private final long eventTimestampSeconds;

        private TaskBatch(S3EventOffset offset, List<String> receiptHandles, long eventTimestampSeconds) {
            this.offset = offset;
            this.receiptHandles = receiptHandles;
            this.eventTimestampSeconds = eventTimestampSeconds;
        }
    }

    private static class ParsedMessage {
        private final String receiptHandle;
        private final List<String> files;
        private final List<TBrokerFileStatus> fileStatuses;
        private final long eventTimestampSeconds;

        private ParsedMessage(String receiptHandle, List<String> files, List<TBrokerFileStatus> fileStatuses,
                long eventTimestampSeconds) {
            this.receiptHandle = receiptHandle;
            this.files = files;
            this.fileStatuses = fileStatuses;
            this.eventTimestampSeconds = eventTimestampSeconds;
        }

        private static ParsedMessage skipped(String receiptHandle) {
            return new ParsedMessage(receiptHandle, Collections.emptyList(), Collections.emptyList(), 0);
        }
    }
}
