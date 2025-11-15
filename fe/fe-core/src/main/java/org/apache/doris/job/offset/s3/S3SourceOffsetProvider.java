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
import org.apache.doris.fs.GlobListResult;
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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Log4j2
public class S3SourceOffsetProvider implements SourceOffsetProvider {
    S3Offset currentOffset;
    String maxEndFile;

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
            GlobListResult globListResult = fileSystem.globListWithLimit(filePath, rfiles, startFile,
                    jobProps.getS3BatchBytes(), jobProps.getS3BatchFiles());

            if (!rfiles.isEmpty()) {
                String bucket = globListResult.getBucket();
                String prefix = globListResult.getPrefix();

                String bucketBase = "s3://" + bucket + "/";
                // Get the path of the last directory
                int lastSlash = prefix.lastIndexOf('/');
                String basePrefix = (lastSlash >= 0) ? prefix.substring(0, lastSlash + 1) : "";
                String filePathBase = bucketBase + basePrefix;
                String joined = rfiles.stream()
                        .map(path -> path.getName().replace(filePathBase, ""))
                        .collect(Collectors.joining(","));

                String normalizedPrefix = basePrefix.endsWith("/")
                        ? basePrefix.substring(0, basePrefix.length() - 1) : basePrefix;
                String finalFileLists = String.format("s3://%s/%s/{%s}", bucket, normalizedPrefix, joined);
                String beginFile = rfiles.get(0).getName().replace(bucketBase, "");
                String lastFile = rfiles.get(rfiles.size() - 1).getName().replace(bucketBase, "");
                offset.setFileLists(finalFileLists);
                offset.setStartFile(beginFile);
                offset.setEndFile(lastFile);
                offset.setFileNum(rfiles.size());
                maxEndFile = globListResult.getMaxFile();
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
        InsertIntoTableCommand insertIntoTableCommand = new InsertIntoTableCommand((LogicalPlan) rewritePlan,
                Optional.empty(), Optional.empty(), Optional.empty(), true, Optional.empty());
        insertIntoTableCommand.setJobId(originCommand.getJobId());
        return insertIntoTableCommand;
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
            List<RemoteFile> objects = new ArrayList<>();
            GlobListResult globListResult = fileSystem.globListWithLimit(filePath, objects, startFile, 1, 1);
            if (globListResult != null && !objects.isEmpty() && StringUtils.isNotEmpty(globListResult.getMaxFile())) {
                maxEndFile = globListResult.getMaxFile();
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public boolean hasMoreDataToConsume() {
        if (currentOffset == null || currentOffset.endFile == null) {
            return true;
        }

        if (maxEndFile != null && currentOffset.endFile.compareTo(maxEndFile) < 0) {
            return true;
        }
        return false;
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
