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

import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;

import java.util.HashMap;
import java.util.Map;

public class S3SourceOffsetProvider implements SourceOffsetProvider {
    S3Offset currentOffset;
    String maxRemoteEndFile;

    @Override
    public String getSourceType() {
        return "s3";
    }

    @Override
    public S3Offset getNextOffset() {
        //todo: listObjects from end file
        return null;
    }

    @Override
    public Offset getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public InsertIntoTableCommand rewriteTvfParams(String sql) {
        S3Offset nextOffset = getNextOffset();
        Map<String, String> props = new HashMap<>();
        //todo: need to change file list to glob string
        props.put("uri", nextOffset.getFileLists().toString());

        NereidsParser parser = new NereidsParser();
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(sql);
        command.rewriteTvfProperties(getSourceType(), props);
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
        if (currentOffset.endFile.compareTo(maxRemoteEndFile) < 0) {
            return true;
        }
        return false;
    }
}
