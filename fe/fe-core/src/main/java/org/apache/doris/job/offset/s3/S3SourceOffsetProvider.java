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

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Log4j2
public class S3SourceOffsetProvider implements SourceOffsetProvider {
    S3Offset currentOffset;
    String maxRemoteEndFile;

    @Override
    public String getSourceType() {
        return "s3";
    }

    @Override
    public S3Offset getNextOffset(StreamingJobProperties jobProps, Map<String, String> properties) {
        Map<String, String> copiedProps = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        copiedProps.putAll(properties);
        S3Offset offset = new S3Offset();
        List<RemoteFile> rfiles = new ArrayList<>();
        String startFile = currentOffset == null ? null : currentOffset.endFile;
        String filePath = null;
        StorageProperties storageProperties = StorageProperties.createPrimary(copiedProps);
        try (RemoteFileSystem fileSystem = FileSystemFactory.get(storageProperties)) {
            String uri = storageProperties.validateAndGetUri(copiedProps);
            filePath = storageProperties.validateAndNormalizeUri(uri);
            maxRemoteEndFile = fileSystem.globListWithLimit(filePath, rfiles, startFile,
                    jobProps.getS3BatchFiles(), jobProps.getS3BatchSize());
            offset.setStartFile(startFile);
            //todo: The path may be in the form of bucket/dir/*/*,
            // but currently only the case where the last segment is * is handled.
            if (!rfiles.isEmpty()) {
                String bucket = rfiles.get(0).getBucket();
                String parentPath = rfiles.get(0).getParentPath();
                String filePaths = rfiles.stream().map(RemoteFile::getName).collect(Collectors.joining(",", "{", "}"));
                String finalFiles = String.format("s3://%s/%s/%s", bucket, parentPath, filePaths);
                offset.setEndFile(String.format("%s/%s", parentPath, rfiles.get(rfiles.size() - 1).getName()));
                offset.setFileLists(finalFiles);
            }
        } catch (Exception e) {
            log.warn("list path exception, path={}", filePath, e);
            throw new RuntimeException(e);
        }
        return offset;
    }

    @Override
    public Offset getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public String getSyncOffset() {
        if (currentOffset != null) {
            return currentOffset.getEndFile();
        }
        return null;
    }

    @Override
    public String getRemoteOffset() {
        return maxRemoteEndFile;
    }

    @Override
    public InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand, Offset runningOffset) {
        S3Offset offset = (S3Offset) runningOffset;
        Map<String, String> props = new HashMap<>();
        // rewrite plan
        Plan rewritePlan = originCommand.getParsedPlan().get().rewriteUp(plan -> {
            if (plan instanceof UnboundTVFRelation) {
                UnboundTVFRelation originTvfRel = (UnboundTVFRelation) plan;
                Map<String, String> oriMap = originTvfRel.getProperties().getMap();
                props.putAll(oriMap);
                props.put("uri", offset.getFileLists());
                return new UnboundTVFRelation(
                        originTvfRel.getRelationId(), originTvfRel.getFunctionName(), new Properties(props));
            }
            return plan;
        });
        return new InsertIntoTableCommand((LogicalPlan) rewritePlan, Optional.empty(), Optional.empty(),
                Optional.empty(), true, Optional.empty());
    }

    @Override
    public void updateOffset(Offset offset) {
        this.currentOffset = (S3Offset) offset;
        this.currentOffset.setFileLists(null);
    }

    @Override
    public void fetchRemoteMeta(Map<String, String> properties) throws Exception {
        Map<String, String> copiedProps = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        copiedProps.putAll(properties);
        StorageProperties storageProperties = StorageProperties.createPrimary(copiedProps);
        String startFile = currentOffset == null ? null : currentOffset.endFile;
        try (RemoteFileSystem fileSystem = FileSystemFactory.get(storageProperties)) {
            String uri = storageProperties.validateAndGetUri(copiedProps);
            String filePath = storageProperties.validateAndNormalizeUri(uri);
            maxRemoteEndFile = fileSystem.globListWithLimit(filePath, new ArrayList<>(), startFile,
                    1, 1);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public boolean hasMoreDataToConsume() {
        if (currentOffset == null) {
            return true;
        }
        if (currentOffset.endFile.compareTo(maxRemoteEndFile) < 0) {
            return true;
        }
        return false;
    }

    @Override
    public Offset deserializeOffset(String offset) {
        return GsonUtils.GSON.fromJson(offset, S3Offset.class);
    }
}
