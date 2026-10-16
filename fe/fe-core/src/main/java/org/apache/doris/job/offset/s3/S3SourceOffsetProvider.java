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

import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.functions.table.S3;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Log4j2
public class S3SourceOffsetProvider implements SourceOffsetProvider {
    private final boolean oneTimeMode;
    private volatile S3Offset noMoreFilesAfterOffset;
    volatile S3Offset currentOffset;
    volatile String maxEndFile;

    public S3SourceOffsetProvider() {
        this.oneTimeMode = false;
    }

    public S3SourceOffsetProvider(StreamingJobProperties jobProperties) {
        this.oneTimeMode = jobProperties.isS3OneTimeMode();
    }

    @Override
    public String getSourceType() {
        return "s3";
    }

    @Override
    public S3Offset getNextOffset(StreamingJobProperties jobProps, Map<String, String> properties) {
        Map<String, String> copiedProps = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        copiedProps.putAll(properties);
        S3Offset offset = new S3Offset();
        String startFile = currentOffset == null ? null : currentOffset.endFile;
        String filePath = null;
        StorageAdapter storageAdapter = StorageAdapter.of(copiedProps);
        try (FileSystem fileSystem = FileSystemFactory.getFileSystem(storageAdapter)) {
            String uri = storageAdapter.validateAndGetUri(copiedProps);
            filePath = storageAdapter.validateAndNormalizeUri(uri);
            GlobListing globListing = fileSystem.globListWithLimit(Location.of(filePath), startFile,
                    jobProps.getS3BatchBytes(), jobProps.getS3BatchFiles());

            List<FileEntry> rfiles = globListing.getFiles();
            if (!rfiles.isEmpty()) {
                String bucket = globListing.getBucket();
                String bucketBase = "s3://" + bucket + "/";
                String beginFile = rfiles.get(0).location().uri().replace(bucketBase, "");
                String lastFile = rfiles.get(rfiles.size() - 1).location().uri().replace(bucketBase, "");
                offset.setFileStatuses(rfiles.stream().map(entry -> new TBrokerFileStatus(
                        entry.location().uri(), entry.isDirectory(), entry.length(), !entry.isDirectory()))
                        .collect(Collectors.toList()));
                offset.setStartFile(beginFile);
                offset.setEndFile(lastFile);
                offset.setFileNum(rfiles.size());
                maxEndFile = globListing.getMaxFile();
                offset.setLastBatch(oneTimeMode && lastFile.equals(globListing.getMaxFile()));
            } else {
                throw new RuntimeException("No new files found in path: " + filePath);
            }
        } catch (Exception e) {
            log.warn("list path exception, path={}", filePath, e);
            throw new RuntimeException(e);
        }
        return offset;
    }

    @Override
    public String getShowCurrentOffset() {
        if (currentOffset != null) {
            Map<String, String> res = new HashMap<>();
            res.put("fileName", currentOffset.getEndFile());
            return new Gson().toJson(res);
        }
        return null;
    }

    @Override
    public String getShowMaxOffset() {
        if (maxEndFile != null) {
            Map<String, String> res = new HashMap<>();
            res.put("fileName", maxEndFile);
            return new Gson().toJson(res);
        }
        return null;
    }

    @Override
    public InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand,
            Offset runningOffset, long taskId) {
        S3Offset offset = (S3Offset) runningOffset;
        return rewriteS3Tvf(originCommand, offset.getFileStatuses());
    }

    // Render the same literal batch in task properties and audit SQL; execution uses file statuses.
    public static String buildFileListUri(List<TBrokerFileStatus> files) {
        Preconditions.checkArgument(!files.isEmpty(), "S3 task batch must contain files");
        String[] paths = files.stream().map(TBrokerFileStatus::getPath).toArray(String[]::new);
        String commonPrefix = StringUtils.getCommonPrefix(paths);
        String baseUri = commonPrefix.substring(0, commonPrefix.lastIndexOf('/') + 1);
        String joined = Arrays.stream(paths).map(path -> escapeGlob(path.substring(baseUri.length())))
                .collect(Collectors.joining(","));
        return escapeGlob(baseUri) + "{" + joined + "}";
    }

    // Selected object keys are literals; only the surrounding braces and separators are glob syntax.
    private static String escapeGlob(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ("\\*?[]{},".indexOf(c) >= 0) {
                escaped.append('\\');
            }
            escaped.append(c);
        }
        return escaped.toString();
    }

    static InsertIntoTableCommand rewriteS3Tvf(InsertIntoTableCommand originCommand,
            List<TBrokerFileStatus> fileStatuses) {
        Preconditions.checkNotNull(fileStatuses, "S3 task batch must contain exact file statuses");
        String fileLists = buildFileListUri(fileStatuses);
        return originCommand.rewriteQuery(plan -> {
            if (plan instanceof UnboundTVFRelation) {
                UnboundTVFRelation tvf = (UnboundTVFRelation) plan;
                Map<String, String> props = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                props.putAll(tvf.getProperties().getMap());
                props.put("uri", fileLists);
                return new LogicalTVFRelation(tvf.getRelationId(), S3.withFiles(new Properties(props), fileStatuses),
                        ImmutableList.of());
            }
            return plan;
        });
    }

    @Override
    public void updateOffset(Offset offset) {
        this.currentOffset = (S3Offset) offset;
        this.currentOffset.setFileStatuses(null);
    }

    @Override
    public void fetchRemoteMeta(Map<String, String> properties) throws Exception {
        Map<String, String> copiedProps = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        copiedProps.putAll(properties);
        StorageAdapter storageAdapter = StorageAdapter.of(copiedProps);
        S3Offset offsetAtScan = currentOffset;
        String startFile = offsetAtScan == null ? null : offsetAtScan.endFile;
        try (FileSystem fileSystem = FileSystemFactory.getFileSystem(storageAdapter)) {
            String uri = storageAdapter.validateAndGetUri(copiedProps);
            String filePath = storageAdapter.validateAndNormalizeUri(uri);
            // debug point: simulate globListWithLimit throwing an IOException (e.g. S3 auth error)
            if (DebugPointUtil.isEnable("S3SourceOffsetProvider.fetchRemoteMeta.error")) {
                throw new java.io.IOException("debug point: simulated S3 auth error");
            }
            GlobListing globListing = fileSystem.globListWithLimit(Location.of(filePath), startFile, 1, 1);
            if (oneTimeMode) {
                noMoreFilesAfterOffset = globListing.getFiles().isEmpty() ? offsetAtScan : null;
            }
            if (!globListing.getFiles().isEmpty() && StringUtils.isNotEmpty(globListing.getMaxFile())) {
                maxEndFile = globListing.getMaxFile();
            }
        }
    }

    @Override
    public boolean hasMoreDataToConsume() {
        if (hasReachedEnd()) {
            return false;
        }
        if (currentOffset == null || currentOffset.endFile == null) {
            return true;
        }

        if (maxEndFile != null && currentOffset.endFile.compareTo(maxEndFile) < 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean hasReachedEnd() {
        S3Offset offset = currentOffset;
        return oneTimeMode && offset != null && (offset.isLastBatch() || noMoreFilesAfterOffset == offset);
    }

    @Override
    public String getPersistInfo() {
        if (currentOffset == null) {
            return null;
        }
        return currentOffset.toSerializedJson();
    }

    @Override
    public void restoreFromPersistInfo(String persistInfo) {
        if (persistInfo == null) {
            return;
        }
        try {
            this.currentOffset = GsonUtils.GSON.fromJson(
                    persistInfo, S3Offset.class);
        } catch (Exception e) {
            log.warn("Failed to restore S3 offset from persistInfo", e);
        }
    }

    @Override
    public void replayIfNeed(StreamingInsertJob job) {
        // If currentOffset was already set by EditLog replay (replayOnCommitted -> updateOffset),
        // it reflects the latest committed state and should not be overwritten by
        // offsetProviderPersist which may be stale (e.g. txn replay runs after ALTER replay).
        if (currentOffset != null) {
            log.info("S3 offset for job {} already set by EditLog replay: endFile={}",
                    job.getJobId(), currentOffset.getEndFile());
            return;
        }
        // Only restore from offsetProviderPersist when currentOffset is null,
        // which means recovery is from checkpoint image without subsequent EditLog replay.
        String persist = job.getOffsetProviderPersist();
        if (persist != null) {
            this.currentOffset = GsonUtils.GSON.fromJson(persist, S3Offset.class);
            log.info("Restored S3 offset from checkpoint for job {}: endFile={}",
                    job.getJobId(), currentOffset.getEndFile());
        }
    }

    @Override
    public Offset deserializeOffset(String offset) {
        return GsonUtils.GSON.fromJson(offset, S3Offset.class);
    }

    /**
     * {"fileName": 1.csv} => S3Offset(endFile=1.csv)
     */
    @Override
    public Offset deserializeOffsetProperty(String offset) {
        if (StringUtils.isBlank(offset)) {
            return null;
        }
        Map<String, String> offsetMap =
                GsonUtils.GSON.fromJson(offset, new TypeToken<HashMap<String, String>>() {}.getType());

        if (offsetMap == null || offsetMap.isEmpty()) {
            return null;
        }

        String fileName = offsetMap.get("fileName");
        if (StringUtils.isBlank(fileName)) {
            return null;
        }

        S3Offset s3Offset = new S3Offset();
        s3Offset.setEndFile(fileName);
        return s3Offset;
    }
}
