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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;

import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
public class S3SourceOffsetProvider implements SourceOffsetProvider {
    String executeSql;
    S3Offset currentOffset;
    String maxRemoteEndFile;
    StreamingJobProperties jobProperties;
    NereidsParser parser;
    String filePath;
    StorageProperties storageProperties;

    @Override
    public void init(String executeSql, StreamingJobProperties jobProperties) {
        //todo: check is already init
        this.executeSql = executeSql;
        this.jobProperties = jobProperties;
        this.parser = new NereidsParser();
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(executeSql);
        UnboundTVFRelation firstTVF = command.getFirstTVF();
        Map<String, String> properties = firstTVF.getProperties().getMap();
        try {
            this.storageProperties = StorageProperties.createPrimary(properties);
            String uri = storageProperties.validateAndGetUri(properties);
            this.filePath = storageProperties.validateAndNormalizeUri(uri);
        } catch (UserException e) {
            throw new RuntimeException("Failed check storage props, " + e.getMessage(), e);
        }
    }

    @Override
    public String getSourceType() {
        return "s3";
    }

    @Override
    public S3Offset getNextOffset() {
        S3Offset offset = new S3Offset();
        List<String> rfiles = new ArrayList<>();
        String startFile = currentOffset == null ? null : currentOffset.endFile;
        try (RemoteFileSystem fileSystem = FileSystemFactory.get(storageProperties)) {
            maxRemoteEndFile = fileSystem.globListWithLimit(filePath, rfiles, startFile,
                    jobProperties.getS3BatchFiles(), jobProperties.getS3BatchSize());
            offset.setStartFile(startFile);
            offset.setEndFile(rfiles.get(rfiles.size() - 1));
            offset.setFileLists(rfiles);
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
    public InsertIntoTableCommand rewriteTvfParams(Offset runningOffset) {
        S3Offset offset = (S3Offset) runningOffset;
        Map<String, String> props = new HashMap<>();
        String finalUri = "{" + String.join(",", offset.getFileLists()) + "}";
        props.put("uri", finalUri);
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(executeSql);
        //todo: command query plan is immutable
        //command.rewriteFirstTvfProperties(getSourceType(), props);
        return command;
    }

    @Override
    public void updateOffset(Offset offset) {
        this.currentOffset = (S3Offset) offset;
    }

    @Override
    public void fetchRemoteMeta() {
        // list object
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
}
