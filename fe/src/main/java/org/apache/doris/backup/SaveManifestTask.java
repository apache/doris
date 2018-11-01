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
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.CommandResult;
import org.apache.doris.common.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class SaveManifestTask extends ResultfulTask {
    private static final Logger LOG = LogManager.getLogger(SaveManifestTask.class);

    private long jobId;
    private String label;
    private String remotePath;
    private String localDirName;

    private String lastestLoadLabel;
    private String lastestDeleteInfo;

    private PathBuilder pathBuilder;
    private CommandBuilder commandBuilder;

    public SaveManifestTask(long jobId, String label, String remotePath, String localDirName,
                            String lastestLoadLabel, String lastestDeleteInfo,
                            PathBuilder pathBuilder, CommandBuilder commandBuilder) {
        this.jobId = jobId;
        this.label = label;
        this.remotePath = remotePath;
        this.localDirName = localDirName;
        this.lastestLoadLabel = lastestLoadLabel;
        this.lastestDeleteInfo = lastestDeleteInfo;
        this.pathBuilder = pathBuilder;
        this.commandBuilder = commandBuilder;
        this.errMsg = null;
    }

    @Override
    public String call() throws Exception {
        try {
            // 1. save and upload manifest
            saveAndUploadManifest();
            // 2. save and upload readable manifest
            saveAndUploadReadableManifest();
        } catch (Exception e) {
            setErrMsg(e);
            return errMsg;
        }

        return null;
    }

    private void saveAndUploadManifest() throws UserException {
        String manifestPath = pathBuilder.manifest();
        DirSaver labelDir = pathBuilder.getRoot();
        Preconditions.checkState(labelDir.getName().equals(localDirName));
        List<? extends Writable> writables = Lists.newArrayList(labelDir);

        String msg = null;
        boolean succeed = false;
        for (int i = 0; i < MAX_RETRY_TIME; i++) {
            try {
                ObjectWriter.write(manifestPath, writables);
                succeed = true;
                break;
            } catch (IOException e) {
                msg = e.getMessage();
                LOG.warn(e.getMessage() + ". retry: " + i);
                try {
                    Thread.sleep(MAX_RETRY_INTERVAL_MS);
                } catch (InterruptedException e1) {
                    LOG.warn(e.getMessage());
                    break;
                }
            }
        }

        if (!succeed) {
            throw new UserException(msg);
        }

        uploadManifest(manifestPath, PathBuilder.MANIFEST_NAME);
    }


    private void saveAndUploadReadableManifest() throws UserException {
        String localReadableManifest = pathBuilder.readableManifest();

        // get list
        List<String> backupedObjs = Lists.newArrayList();
        DirSaver root = pathBuilder.getRoot();
        for (FileSaverI rootChild : root.getChildren()) {
            if (rootChild instanceof FileSaver) {
                continue;
            }

            DirSaver dbDir = (DirSaver) rootChild;
            for (FileSaverI dbChild : dbDir.getChildren()) {
                if (dbChild instanceof FileSaver) {
                    continue;
                }

                DirSaver tableDir = (DirSaver) dbChild;
                String tableName = tableDir.getName();
                boolean hasDir = false;
                for (FileSaverI tableChild : tableDir.getChildren()) {
                    if (tableChild instanceof FileSaver) {
                        continue;
                    }

                    hasDir = true;
                    String partitionName = tableChild.getName();

                    String res = PathBuilder.createPath(tableName, partitionName);
                    backupedObjs.add(res);
                    LOG.debug("get backuped obj: {}", res);
                } // end for tables

                if (!hasDir) {
                    // non-olap table(mysql etc)
                    backupedObjs.add(tableName);
                }
            } // end for dbs;
        } // end for labels

        if (backupedObjs.isEmpty()) {
            throw new UserException("nothing backuped??!!, job: " + jobId);
        }

        // add last load label and last delete info
        backupedObjs.add("Last Load Label: " + lastestLoadLabel);
        backupedObjs.add("Last Delete: " + lastestDeleteInfo);

        String msg = null;
        boolean succeed = false;
        for (int i = 0; i < MAX_RETRY_TIME; i++) {
            try {
                ObjectWriter.writeReadable(localReadableManifest, backupedObjs);
                succeed = true;
                break;
            } catch (IOException e) {
                msg = e.getMessage();
                LOG.warn(e.getMessage() + ". retry: " + i);
                try {
                    Thread.sleep(MAX_RETRY_INTERVAL_MS);
                } catch (InterruptedException e1) {
                    LOG.warn(e.getMessage());
                    break;
                }
            }
        }

        if (!succeed) {
            LOG.warn("Failed to save readable manifest. job: {}", jobId);
            throw new UserException(msg);
        }

        uploadManifest(localReadableManifest, PathBuilder.READABLE_MANIFEST_NAME);
    }

    private void uploadManifest(String manifestFile, String fileName) throws UserException {
        String uploadCmd = null;
        String msg = null;
        boolean succeed = false;
        for (int i = 0; i < MAX_RETRY_TIME; i++) {
            try {
                String readableManifestPath = PathBuilder.createPath(remotePath, label, fileName);
                uploadCmd = commandBuilder.uploadCmd(label, manifestFile, readableManifestPath);
            } catch (IOException e) {
                msg = e.getMessage();
                LOG.warn("{}. job[{}]. retry: {}", errMsg, jobId, i);
                try {
                    Thread.sleep(MAX_RETRY_INTERVAL_MS);
                    continue;
                } catch (InterruptedException e1) {
                    LOG.warn(e.getMessage());
                    break;
                }
            }

            String[] envp = { "LC_ALL=" + Config.locale };
            CommandResult result = Util.executeCommand(uploadCmd, envp);
            if (result.getReturnCode() != 0) {
                msg = "failed to upload meta files[" + result + "]. backup job[" + jobId + "]";
                LOG.warn("{}. job[{}]. retry: {}", errMsg, jobId, i);
                try {
                    Thread.sleep(MAX_RETRY_INTERVAL_MS);
                    continue;
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage());
                    break;
                }
            }

            succeed = true;
        }

        if (!succeed) {
            throw new UserException(msg);
        }
    }
}
