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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.BaseTableStream.StreamScanType;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateStreamInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * create multi table materialized view
 */
public class CreateMTMVCommand extends Command implements ForwardWithSync {

    public static final Logger LOG = LogManager.getLogger(CreateMTMVCommand.class);
    private final CreateMTMVInfo createMTMVInfo;

    /**
     * constructor
     */
    public CreateMTMVCommand(CreateMTMVInfo createMTMVInfo) {
        super(PlanType.CREATE_MTMV_COMMAND);
        this.createMTMVInfo = Objects.requireNonNull(createMTMVInfo, "require CreateMTMVInfo object");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        createMTMVInfo.analyze(ctx);
        Env.getCurrentEnv().createTable(this.createMTMVInfo);
        List<String> createdStreamNames = new ArrayList<>();
        try {
            createIvmStreams(ctx, createdStreamNames);
        } catch (Exception e) {
            // Rollback: drop any streams we already created, then force-drop the MTMV
            dropStreamsForce(createdStreamNames);
            dropMtmvForce();
            throw e;
        }
    }

    /**
     * If the MTMV enables IVM (explicit INCREMENTAL refresh), automatically
     * creates a stream for each base table so the delta rewrite path can
     * read binlog data through the existing stream scan infrastructure.
     */
    private void createIvmStreams(ConnectContext ctx,
            List<String> createdStreamNames) throws Exception {
        if (!createMTMVInfo.isEnableIvm()) {
            return;
        }
        MTMVRelation relation = createMTMVInfo.getRelation();
        if (relation == null) {
            LOG.warn("IVM: no relation found for MTMV {}, skip stream creation",
                    createMTMVInfo.getTableName());
            return;
        }
        Set<BaseTableInfo> baseTables = relation.getBaseTables();
        if (baseTables == null || baseTables.isEmpty()) {
            return;
        }
        String mvDbName = createMTMVInfo.getDbName();
        String mvName = createMTMVInfo.getTableName();
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(mvDbName);
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException(mvName);
        Set<TableNameInfo> excluded = mtmv.getExcludedTriggerTables();
        for (BaseTableInfo baseTableInfo : baseTables) {
            // Skip excluded trigger tables — they don't participate in incremental refresh
            if (excluded != null && MTMVPartitionUtil.isTableExcluded(excluded,
                    new TableNameInfo(baseTableInfo.getCtlName(),
                            baseTableInfo.getDbName(), baseTableInfo.getTableName()))) {
                LOG.info("IVM: skipping stream creation for excluded trigger table {}",
                        baseTableInfo.getTableName());
                continue;
            }
            TableIf table = MTMVUtil.getTable(baseTableInfo);
            createTableStream(ctx, db, mtmv, table, baseTableInfo.getDbName(), createdStreamNames);
        }
    }

    /**
     * Creates a stream for a single base table of an IVM MTMV.
     * Drops any existing stream first, then creates a fresh one.
     *
     * @param ctx connection context
     * @param mvDb database containing the MTMV
     * @param mtmv the MTMV
     * @param baseTable the base table to create stream for
     * @param baseTableDbName the database name of the base table
     * @param createdStreamNames output list to collect created stream names (for rollback)
     */
    static void createTableStream(ConnectContext ctx, Database mvDb, MTMV mtmv, TableIf baseTable,
            String baseTableDbName, List<String> createdStreamNames) throws Exception {
        String mvDbName = mvDb.getFullName();
        long mvId = mtmv.getId();
        String streamName = IvmUtil.streamName(mvId, baseTable.getName());
        TableNameInfo streamTableName = new TableNameInfo(
                InternalCatalog.INTERNAL_CATALOG_NAME, mvDbName, streamName);
        TableNameInfo baseTableName = new TableNameInfo(
                InternalCatalog.INTERNAL_CATALOG_NAME, baseTableDbName, baseTable.getName());
        // Drop old stream if exists, so validation always runs on the fresh stream
        TableIf oldStream = mvDb.getTableNullable(streamName);
        if (oldStream != null) {
            Env.getCurrentInternalCatalog().dropTableWithoutCheck(
                    mvDb, (Table) oldStream, false, true /* forceDrop */);
        }
        Map<String, String> streamProps = new HashMap<>();
        streamProps.put(PropertyAnalyzer.PROPERTIES_STREAM_SHOW_INITIAL_ROWS, "true");
        if (baseTable instanceof OlapTable && ((OlapTable) baseTable).isUniqKeyMergeOnWrite()) {
            streamProps.put(PropertyAnalyzer.PROPERTIES_STREAM_TYPE,
                    StreamScanType.MIN_DELTA.name().toLowerCase());
        }
        CreateStreamInfo streamInfo = new CreateStreamInfo(
                false /* ifNotExists */, false /* orReplace */,
                streamTableName, baseTableName,
                streamProps, "" /* comment */);
        streamInfo.validate(ctx);
        Env.getCurrentEnv().getInternalCatalog().createTableStream(
                new CreateStreamCommand(streamInfo));
        if (createdStreamNames != null) {
            createdStreamNames.add(streamName);
        }
        LOG.info("IVM: auto-created stream {} for MTMV {} base table {}",
                streamName, mvId, baseTable.getName());
    }

    private void dropStreamsForce(List<String> streamNames) {
        String mvDbName = createMTMVInfo.getDbName();
        Database db = null;
        try {
            db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(mvDbName);
        } catch (Exception ignored) {
            return;
        }
        for (String streamName : streamNames) {
            try {
                TableIf t = db.getTableOrAnalysisException(streamName);
                Env.getCurrentInternalCatalog().dropTableWithoutCheck(
                        db, (Table) t, false, true /* forceDrop */);
            } catch (Exception ignored) {
                LOG.warn("IVM: failed to force-drop stream {} during rollback, ignored", streamName);
            }
        }
    }

    private void dropMtmvForce() {
        try {
            Database db = Env.getCurrentInternalCatalog()
                    .getDbOrAnalysisException(createMTMVInfo.getDbName());
            TableIf t = db.getTableOrAnalysisException(
                    createMTMVInfo.getTableName());
            Env.getCurrentInternalCatalog().dropTableWithoutCheck(
                    db, (Table) t, false, true /* forceDrop */);
        } catch (Exception ignored) {
            LOG.warn("IVM: failed to force-drop MTMV {} during rollback, ignored",
                    createMTMVInfo.getTableName());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMTMVCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    public CreateMTMVInfo getCreateMTMVInfo() {
        return createMTMVInfo;
    }

}
