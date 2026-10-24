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

import java.util.List;

public interface AbstractInsertOverwriteManager {
    long registerTask(TableIf targetTable, List<String> tempPartitionNames) throws Exception;

    long registerTaskGroup(TableIf targetTable) throws Exception;

    void registerTaskInGroup(long groupId, long taskId) throws Exception;

    void taskGroupSuccess(long groupId, OlapTable targetTable, boolean forceDropPartition) throws DdlException;

    /**
     * The replacement half of {@link #taskGroupSuccess}: makes every replacement the group recorded visible.
     * A caller that publishes the group itself calls this and then {@link #finishTaskGroup}, so that the task
     * bookkeeping -- an edit-log write per task, each waiting for its journal -- runs outside whatever lock the
     * caller holds the replacement under. A manager whose owning frontend runs the whole sequence behind one
     * call replaces there and finishes there.
     */
    void replacePartitionsOfTaskGroup(long groupId, OlapTable targetTable, boolean forceDropPartition)
            throws DdlException;

    /** The bookkeeping half of {@link #taskGroupSuccess}, after the replacement it follows. */
    void finishTaskGroup(long groupId);

    void taskSuccess(long taskId) throws Exception;

    void taskGroupFail(long groupId) throws Exception;

    void taskFail(long taskId) throws Exception;

    void recordRunningTableOrException(DatabaseIf db, TableIf table) throws Exception;

    void dropRunningRecord(DatabaseIf db, TableIf targetTable) throws Exception;
}
