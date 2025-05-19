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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.nereids.trees.plans.commands.info.CancelMTMVTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PauseMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ResumeMTMVInfo;

import java.util.Optional;

/**
 * Contains all operations that affect the mtmv
 */
public interface MTMVHookService {
    /**
     * triggered after create mtmv, only once
     *
     * @param mtmv
     */
    void postCreateMTMV(MTMV mtmv);

    /**
     * triggered when playing `create mtmv` logs
     * When triggered, db has not completed playback yet, so use dbId as param
     *
     * @param mtmv
     * @param dbId when load from image, table.getDatabase() will be null, so need dbId as param
     */
    void registerMTMV(MTMV mtmv, Long dbId);

    /**
     * triggered when playing `drop mtmv` logs
     *
     * @param mtmv
     */
    void unregisterMTMV(MTMV mtmv);

    /**
     * triggered when refresh mtmv
     *
     * @param info
     * @throws DdlException
     * @throws MetaNotFoundException
     */
    void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException, JobException;

    /**
     * triggered when mtmv task finish
     *
     * @param mtmv
     * @param relation
     * @param task
     */
    void refreshComplete(MTMV mtmv, MTMVRelation relation, MTMVTask task);

    /**
     * Triggered when baseTable is dropped
     *
     * @param table
     */
    void dropTable(Table table);

    /**
     * Triggered when baseTable is altered
     *
     * @param oldTableInfo info before alter
     * @param newTableInfo info after alter
     * @param isReplace
     */
    void alterTable(BaseTableInfo oldTableInfo, Optional<BaseTableInfo> newTableInfo, boolean isReplace);

    /**
     * Triggered when pause mtmv
     *
     * @param info
     */
    void pauseMTMV(PauseMTMVInfo info) throws MetaNotFoundException, DdlException, JobException;

    /**
     * Triggered when resume mtmv
     *
     * @param info
     */
    void resumeMTMV(ResumeMTMVInfo info) throws MetaNotFoundException, DdlException, JobException;

    /**
     * cancel mtmv task
     *
     * @param info
     */
    void cancelMTMVTask(CancelMTMVTaskInfo info) throws DdlException, MetaNotFoundException, JobException;
}
