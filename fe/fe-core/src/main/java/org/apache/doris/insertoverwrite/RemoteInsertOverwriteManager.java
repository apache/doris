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

package org.apache.doris.insertoverwrite;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class RemoteInsertOverwriteManager implements AbstractInsertOverwriteManager {
    private static final Logger LOG = LogManager.getLogger(RemoteInsertOverwriteManager.class);
    private RemoteDorisExternalCatalog catalog;

    public RemoteInsertOverwriteManager(RemoteDorisExternalCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public long registerTask(TableIf targetTable, List<String> tempPartitionNames) throws Exception {
        return  catalog.getFeServiceClient()
                .registerTask(InternalCatalog.INTERNAL_CATALOG_NAME,
                        targetTable.getDatabase().getFullName(), targetTable.getName(), tempPartitionNames);
    }

    @Override
    public long registerTaskGroup(TableIf targetTable) throws Exception {
        return catalog.getFeServiceClient()
                .registerTaskGroup(InternalCatalog.INTERNAL_CATALOG_NAME,
                        targetTable.getDatabase().getFullName(), targetTable.getName());
    }

    @Override
    public void registerTaskInGroup(long groupId, long taskId) throws Exception {
        catalog.getFeServiceClient().registerTaskInGroup(groupId, taskId);
    }

    @Override
    public void taskGroupSuccess(long groupId, OlapTable targetTable) throws DdlException {
        catalog.getFeServiceClient()
                .taskGroupSuccess(InternalCatalog.INTERNAL_CATALOG_NAME,
                        targetTable.getDatabase().getFullName(), targetTable.getName(), groupId);
    }

    @Override
    public void taskSuccess(long taskId) throws Exception {
        catalog.getFeServiceClient().taskSuccess(taskId);
    }

    @Override
    public void taskFail(long taskId) throws Exception {
        catalog.getFeServiceClient().taskFail(taskId);
    }

    @Override
    public void taskGroupFail(long groupId) throws Exception {
        catalog.getFeServiceClient().taskGroupFail(groupId);
    }

    @Override
    public void recordRunningTableOrException(DatabaseIf db, TableIf table) throws Exception {
        catalog.getFeServiceClient()
                .recordRunningTableOrException(InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                        table.getName());
    }

    @Override
    public void dropRunningRecord(DatabaseIf db, TableIf targetTable) throws Exception {
        catalog.getFeServiceClient()
                .dropRunningRecord(InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(), targetTable.getName());
    }
}
