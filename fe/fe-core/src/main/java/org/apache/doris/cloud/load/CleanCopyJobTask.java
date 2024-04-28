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

package org.apache.doris.cloud.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud.FinishCopyRequest.Action;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CleanCopyJobTask {
    private static final Logger LOG = LogManager.getLogger(CleanCopyJobTask.class);
    private RemoteBase.ObjectInfo objectInfo;
    private String stageId;
    private StagePB.StageType stageType;
    private long tableId;
    private String copyId;
    private List<String> loadFiles;

    public CleanCopyJobTask(ObjectInfo objectInfo, String stageId, StageType stageType, long tableId,
            String copyId, List<String> loadFiles) {
        this.objectInfo = objectInfo;
        this.stageId = stageId;
        this.stageType = stageType;
        this.tableId = tableId;
        this.copyId = copyId;
        this.loadFiles = loadFiles;
    }

    public void execute() {
        if (!Config.cloud_delete_loaded_internal_stage_files) {
            return;
        }
        RemoteBase remote = null;
        try {
            remote = RemoteBase.newInstance(objectInfo);
            remote.deleteObjects(loadFiles);
            ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                    .finishCopy(stageId, stageType, tableId, copyId, 0, Action.REMOVE);
        } catch (Throwable e) {
            LOG.warn("Failed delete internal stage files={}", loadFiles, e);
        } finally {
            loadFiles = null;
            if (remote != null) {
                remote.close();
            }
        }
    }
}
