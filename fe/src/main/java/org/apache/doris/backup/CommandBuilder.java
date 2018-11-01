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

package org.apache.doris.backup;

import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public class CommandBuilder implements Writable {
    private static final Logger LOG = LogManager.getLogger(CommandBuilder.class);
    
    private static final String DORIS_HOME = System.getenv("DORIS_HOME");
    private static final String BACKUP_PLUGIN_CMD = DORIS_HOME + Config.backup_plugin_path;

    private static final String ACTION_UPLOAD = "upload";
    private static final String ACTION_DOWNLOAD = "download";
    private static final String ACTION_REMOVE = "remove";

    private String remotePropFilePath;
    private Map<String, String> remoteProperties;

    public CommandBuilder() {
        remoteProperties = Maps.newHashMap();
    }

    private CommandBuilder(String remotePropFilePath, Map<String, String> remoteProperties) {
        this.remotePropFilePath = remotePropFilePath;
        this.remoteProperties = remoteProperties;
    }

    public static CommandBuilder create(String absolutePath, Map<String, String> remoteProperties) throws IOException {
        Preconditions.checkArgument(!remoteProperties.isEmpty());
        createJsonFile(absolutePath, remoteProperties);
        return new CommandBuilder(absolutePath, remoteProperties);
    }

    public String uploadCmd(String label, String src, String dest) throws IOException {
        checkAndRecreateProp();
        String cmd = Joiner.on(" ").join(BACKUP_PLUGIN_CMD, label, ACTION_UPLOAD, src, dest, remotePropFilePath);
        LOG.debug("build cmd: {}", cmd);
        return cmd;
    }

    public String downloadCmd(String label, String local, String remote) throws IOException {
        checkAndRecreateProp();
        String cmd = Joiner.on(" ").join(BACKUP_PLUGIN_CMD, label, ACTION_DOWNLOAD, local, remote, remotePropFilePath);
        LOG.debug("build cmd: {}", cmd);
        return cmd;
    }

    public String removeFileCmd(String label, String file) throws IOException {
        checkAndRecreateProp();
        String cmd = Joiner.on(" ").join(BACKUP_PLUGIN_CMD, label, ACTION_REMOVE, file, remotePropFilePath);
        LOG.debug("build cmd: {}", cmd);
        return cmd;
    }

    private void checkAndRecreateProp() throws IOException {
        File jsonFile = new File(remotePropFilePath);
        if (!jsonFile.exists()) {
            createJsonFile(remotePropFilePath, remoteProperties);
        }
    }

    private static void createJsonFile(String remotePropFilePath, Map<String, String> remoteProperties)
            throws IOException {
        ObjectWriter.writeAsJson(remotePropFilePath, remoteProperties);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, remotePropFilePath);
        int size = remoteProperties.size();
        out.writeInt(size);
        for (Map.Entry<String, String> entry : remoteProperties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        remotePropFilePath = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            remoteProperties.put(key, value);
        }
    }
}
