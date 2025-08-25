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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.load.CloudLoadManager;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.LoadProcDir;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.Util;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.LoadJob;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * show load command
 */
public class ShowLoadCommand extends ShowCommand {
    // LOAD_TITLE_NAMES copy from org.apache.doris.common.proc.LoadProcDir
    public static final ImmutableList<String> LOAD_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("State").add("Progress")
            .add("Type").add("EtlInfo").add("TaskInfo").add("ErrorMsg").add("CreateTime")
            .add("EtlStartTime").add("EtlFinishTime").add("LoadStartTime").add("LoadFinishTime")
            .add("URL").add("JobDetails").add("TransactionId").add("ErrorTablets").add("User").add("Comment")
            .build();

    // STREAM_LOAD_TITLE_NAMES copy from org.apache.doris.analysis.org.apache.doris.analysis
    public static final ImmutableList<String> STREAM_LOAD_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Label").add("Db").add("Table")
            .add("ClientIp").add("Status").add("Message").add("Url").add("TotalRows")
            .add("LoadedRows").add("FilteredRows").add("UnselectedRows").add("LoadBytes")
            .add("StartTime").add("FinishTime").add("User").add("Comment")
            .build();
    protected String labelValue;
    protected String stateValue;
    protected boolean isAccurateMatch;
    protected String copyIdValue;
    protected String tableNameValue;
    protected String fileValue;
    protected boolean isCopyIdAccurateMatch;
    protected boolean isTableNameAccurateMatch;
    protected boolean isFilesAccurateMatch;
    private final Expression wildWhere;
    private final long limit;
    private final long offset;
    private final List<OrderKey> orderKeys;
    private String dbName;
    private ArrayList<OrderByPair> orderByPairs;
    private boolean isStreamLoad;

    /**
     * StreamLoadState
     */
    public enum StreamLoadState {
        SUCCESS,
        FAIL
    }

    /**
     * constructor for show load
     */
    public ShowLoadCommand(Expression wildWhere, List<OrderKey> orderKeys, long limit, long offset, String dbName) {
        super(PlanType.SHOW_LOAD_COMMAND);
        this.wildWhere = wildWhere;
        this.orderKeys = orderKeys;
        this.limit = limit;
        this.offset = offset;
        this.dbName = dbName;
        this.isStreamLoad = false;

        this.copyIdValue = null;
        this.isCopyIdAccurateMatch = false;
    }

    /**
     * constructor for show load
     */
    public ShowLoadCommand(Expression wildWhere, List<OrderKey> orderKeys, long limit,
                               long offset, String dbName, boolean isStreamLoad) {
        super(PlanType.SHOW_LOAD_COMMAND);
        this.wildWhere = wildWhere;
        this.orderKeys = orderKeys;
        this.limit = limit;
        this.offset = offset;
        this.dbName = dbName;
        this.isStreamLoad = isStreamLoad;

        this.copyIdValue = null;
        this.isCopyIdAccurateMatch = false;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ImmutableList<String> titles = isStreamLoad ? STREAM_LOAD_TITLE_NAMES : LOAD_TITLE_NAMES;
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : titles) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    protected boolean isAccurateMatch() {
        return isAccurateMatch;
    }

    protected boolean isStreamLoad() {
        return isStreamLoad;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        return this.orderByPairs;
    }

    private org.apache.doris.load.loadv2.JobState getStateV2() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }
        return org.apache.doris.load.loadv2.JobState.valueOf(stateValue);
    }

    private StreamLoadState getStreamLoadState() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }

        StreamLoadState state = null;
        try {
            state = StreamLoadState.valueOf(stateValue);
        } catch (Exception e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
        return state;
    }

    /**
     * handle show load
     */
    private ShowResultSet handleShowLoad(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // first validate the where
        boolean valid = validate(ctx);
        if (!valid) {
            throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\","
                    + " or LABEL LIKE \"matcher\", " + " or STATE = \"PENDING|ETL|LOADING|FINISHED|CANCELLED\", "
                    + " or compound predicate with operator AND");
        }

        // then process the order by
        ImmutableList<String> titles = isStreamLoad ? STREAM_LOAD_TITLE_NAMES : LOAD_TITLE_NAMES;
        orderByPairs = getOrderByPairs(orderKeys, titles);

        Set<EtlJobType> jobTypes = Sets.newHashSet(EnumSet.allOf(EtlJobType.class));
        jobTypes.remove(EtlJobType.COPY);

        // get the load info
        Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), this.getClass().getSimpleName());
        Env env = ctx.getEnv();
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);
        long dbId = db.getId();
        List<List<Comparable>> loadInfos = Lists.newArrayList();
        Set<String> statesValue = getStates() == null ? null : getStates().stream()
                .map(entity -> entity.name())
                .collect(Collectors.toSet());
        if (!Config.isCloudMode()) {
            loadInfos.addAll(env.getLoadManager().getLoadJobInfosByDb(dbId, labelValue, isAccurateMatch, statesValue));
        } else {
            loadInfos.addAll(((CloudLoadManager) env.getLoadManager())
                    .getLoadJobInfosByDb(dbId, labelValue,
                        isAccurateMatch, statesValue, jobTypes, copyIdValue, isCopyIdAccurateMatch,
                        tableNameValue, isTableNameAccurateMatch, fileValue, isFilesAccurateMatch));
        }
        // add the nerieds load info
        JobManager loadMgr = env.getJobManager();
        loadInfos.addAll(loadMgr.getLoadJobInfosByDb(dbId, db.getFullName(), labelValue,
                isAccurateMatch, getStateV2(), db.getCatalog().getName()));

        // for stream load
        List<List<Comparable>> streamLoadRecords = new ArrayList<>();
        if (isStreamLoad) {
            streamLoadRecords = env.getStreamLoadRecordMgr()
                    .getStreamLoadRecordByDb(dbId, labelValue, isAccurateMatch, getStreamLoadState());
        }

        ListComparator<List<Comparable>> comparator;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<>(0);
        }
        List<List<Comparable>> showLoadInfos = isStreamLoad ? streamLoadRecords : loadInfos;
        Collections.sort(showLoadInfos, comparator);

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> loadInfo : showLoadInfos) {
            List<String> oneInfo = new ArrayList<>(loadInfo.size());

            // replace QUORUM_FINISHED -> FINISHED
            if (loadInfo.get(LoadProcDir.STATE_INDEX).equals(LoadJob.JobState.QUORUM_FINISHED.name())) {
                loadInfo.set(LoadProcDir.STATE_INDEX, LoadJob.JobState.FINISHED.name());
            }

            for (Comparable element : loadInfo) {
                oneInfo.add(element.toString());
            }
            rows.add(oneInfo);
        }

        // filter by limit
        rows = applyLimit(limit, offset, rows);

        return new ShowResultSet(getMetaData(), rows);
    }

    @VisibleForTesting
    protected boolean validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // analyze where clause if not null
        if (wildWhere != null) {
            if (wildWhere instanceof CompoundPredicate) {
                return analyzeCompoundPredicate(wildWhere);
            } else {
                return analyzeSubPredicate(wildWhere);
            }
        }
        return true;
    }

    private Set<LoadJob.JobState> getStates() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }

        Set<LoadJob.JobState> states = new HashSet<LoadJob.JobState>();
        LoadJob.JobState state = LoadJob.JobState.valueOf(stateValue);
        states.add(state);

        if (state == LoadJob.JobState.FINISHED) {
            states.add(LoadJob.JobState.QUORUM_FINISHED);
        }
        return states;
    }

    private boolean analyzeCompoundPredicate(Expression expr) throws AnalysisException {
        if (expr instanceof CompoundPredicate) {
            if (!(expr instanceof And)) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            checkPredicateName(expr.child(0), expr.child(1));
            return analyzeSubPredicate(expr.child(0)) && analyzeSubPredicate(expr.child(1));
        } else {
            return analyzeSubPredicate(expr);
        }
    }

    private void checkPredicateName(Expression leftChild, Expression rightChild) throws AnalysisException {
        String leftKey = ((UnboundSlot) leftChild.child(0)).getName();
        String rightKey = ((UnboundSlot) rightChild.child(0)).getName();
        if (leftKey.equals(rightKey)) {
            throw new AnalysisException("column names on both sides of operator AND should be different");
        }
    }

    private boolean analyzeSubPredicate(Expression expr) {
        if (expr == null) {
            return true;
        }

        boolean hasLabel = false;
        boolean hasState = false;

        if (!(expr instanceof EqualTo) && !(expr instanceof Like)) {
            return false;
        }

        // left child
        if (!(expr.child(0) instanceof UnboundSlot)) {
            return false;
        }

        String leftKey = ((UnboundSlot) expr.child(0)).getName();
        if (leftKey.equalsIgnoreCase("label")) {
            hasLabel = true;
        } else if (leftKey.equalsIgnoreCase("state")) {
            hasState = true;
        } else {
            return false;
        }

        if (hasState && !(expr instanceof EqualTo)) {
            return false;
        }

        if (hasLabel && expr instanceof EqualTo) {
            isAccurateMatch = true;
        }

        // right child
        if (!(expr.child(1) instanceof StringLikeLiteral)) {
            return false;
        }

        String rightValue = ((StringLikeLiteral) expr.child(1)).getStringValue();
        if (Strings.isNullOrEmpty(rightValue)) {
            return false;
        }

        if (hasLabel && !isAccurateMatch && !rightValue.contains("%")) {
            rightValue = "%" + rightValue + "%";
        }

        if (hasLabel) {
            labelValue = rightValue;
        } else if (hasState) {
            stateValue = rightValue.toUpperCase();

            try {
                if (isStreamLoad) {
                    StreamLoadState.valueOf(stateValue);
                } else {
                    LoadJob.JobState.valueOf(stateValue);
                }
            } catch (Exception e) {
                return false;
            }
        }

        return true;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowLoad(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowLoadCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
