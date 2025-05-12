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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.load.CloudLoadManager;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.proc.LoadProcDir;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.Util;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadJob;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

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
 * ShowLoadCommand
 */
public class ShowLoadCommand extends ShowCommand {

    private static final ImmutableList<String> LOAD_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("State").add("Progress")
            .add("Type").add("EtlInfo").add("TaskInfo").add("ErrorMsg").add("CreateTime")
            .add("EtlStartTime").add("EtlFinishTime").add("LoadStartTime").add("LoadFinishTime")
            .add("URL").add("JobDetails").add("TransactionId").add("ErrorTablets").add("User").add("Comment")
            .build();

    private static final ImmutableList<String> SREAMD_LOAD_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Label").add("Db").add("Table")
            .add("ClientIp").add("Status").add("Message").add("Url").add("TotalRows")
            .add("LoadedRows").add("FilteredRows").add("UnselectedRows").add("LoadBytes")
            .add("StartTime").add("FinishTime").add("User").add("Comment")
            .build();

    /**
     * JobState
     */
    public enum JobState {
        UNKNOWN, // only for show load state value check, details, see LoadJobV2's JobState
        PENDING,
        ETL,
        LOADING,
        FINISHED,
        QUORUM_FINISHED,
        CANCELLED
    }

    /**
     * StreamLoadState
     */
    public enum StreamLoadState {
        SUCCESS,
        FAIL
    }

    protected boolean isAccurateMatch;
    protected String labelValue;
    protected String stateValue;
    protected String copyIdValue;
    protected String tableNameValue;
    protected String fileValue;
    protected boolean isCopyIdAccurateMatch;
    protected boolean isTableNameAccurateMatch;
    protected boolean isFilesAccurateMatch;

    private String catalog;
    private String dbName;
    private final List<OrderKey> orderByElements;
    private List<OrderByPair> orderByPairs;
    private final Expression wildWhere;
    private String likePattern;
    private final long limit;
    private final long offset;
    private final boolean isStreamLoad;

    /**
     * ShowLoadCommand
     */
    public ShowLoadCommand(String catalog, String db, List<OrderKey> orderByElements,
                           Expression whildWhere, String likePattern, long limit, long offset, boolean isStreamLoad) {
        super(PlanType.SHOW_LOAD_COMMAND);
        this.catalog = catalog;
        this.dbName = db;
        this.orderByElements = orderByElements;
        this.wildWhere = whildWhere;
        this.likePattern = likePattern;
        this.limit = limit;
        this.offset = offset;
        this.isStreamLoad = isStreamLoad;
        this.labelValue = null;
        this.stateValue = null;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        if (isStreamLoad) {
            return handleShowStreamLoad();
        }
        return handleShowLoad(ctx);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = ctx.getDefaultCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FOR_CATALOG);
            }
        }

        if (wildWhere != null) {
            if (wildWhere instanceof CompoundPredicate) {
                CompoundPredicate predicate = (CompoundPredicate) wildWhere;
                if (!(wildWhere instanceof And)) {
                    throw new AnalysisException("Only allow compound predicate with operator AND");
                }
                Expression left = predicate.child(0);
                Expression right = predicate.child(1);
                checkAndExpression(left, right);
                analyzeSubExpr(left);
                analyzeSubExpr(right);
            } else {
                analyzeSubExpr(wildWhere);
            }
        }

        // order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderKey orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof Slot)) {
                    throw new AnalysisException("Should order by column");
                }
                Slot slot = (Slot) orderByElement.getExpr();
                int index;
                if (isStreamLoad) {
                    index = analyzeStreamLoadColumn(slot.getName());
                } else {
                    index = analyzeLoadColumn(slot.getName());
                }
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.isAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    private void checkAndExpression(Expression left, Expression right) throws AnalysisException {
        if (left.child(0).equals(right.child(0))) {
            throw new AnalysisException("names on both sides of operator AND should be diffrent");
        }
    }

    protected void analyzeSubExpr(Expression expression) throws AnalysisException {
        boolean isValid = isWhereClauseValid(expression);
        if (!isValid) {
            if (isStreamLoad) {
                throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\","
                    + " or LABEL LIKE \"matcher\", " + " or STATUS = \"SUCCESS|FAIL\", "
                    + " or compound predicate with operator AND");
            } else {
                throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\","
                    + " or LABEL LIKE \"matcher\", " + " or STATE = \"PENDING|ETL|LOADING|FINISHED|CANCELLED\", "
                    + " or compound predicate with operator AND");
            }
        }
    }

    private boolean isWhereClauseValid(Expression expr) {
        if (expr == null) {
            return true;
        }

        boolean hasLabel = false;
        boolean hasState = false;

        if (!((expr instanceof EqualTo) || (expr instanceof Like))) {
            return false;
        }

        if (!(expr.child(0) instanceof Slot)) {
            return false;
        }

        String leftKey = ((Slot) expr.child(0)).getName();
        if (leftKey.equalsIgnoreCase("label")) {
            hasLabel = true;
        } else if (!isStreamLoad && leftKey.equalsIgnoreCase("state")) {
            hasState = true;
        } else if (isStreamLoad && leftKey.equalsIgnoreCase("status")) {
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
        if (!(expr.child(1) instanceof StringLiteral)) {
            return false;
        }

        String value = ((StringLiteral) expr.child(1)).getStringValue();
        if (Strings.isNullOrEmpty(value)) {
            return false;
        }

        if (hasLabel && !isAccurateMatch && !value.contains("%")) {
            value = "%" + value + "%";
        }

        if (hasLabel) {
            labelValue = value;
        } else if (hasState) {
            stateValue = value.toUpperCase();
            try {
                if (isStreamLoad) {
                    StreamLoadState.valueOf(stateValue);
                } else {
                    JobState.valueOf(stateValue);
                }

            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    private int analyzeStreamLoadColumn(String columnName) throws AnalysisException {
        for (String title : SREAMD_LOAD_TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return SREAMD_LOAD_TITLE_NAMES.indexOf(title);
            }
        }
        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    private static int analyzeLoadColumn(String columnName) throws AnalysisException {
        for (String title : LOAD_TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return LOAD_TITLE_NAMES.indexOf(title);
            }
        }
        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    private ShowResultSet handleShowLoad(ConnectContext ctx) throws AnalysisException {
        Set<EtlJobType> jobTypes = Sets.newHashSet(EnumSet.allOf(EtlJobType.class));
        jobTypes.remove(EtlJobType.COPY);

        Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), this.getClass().getSimpleName());
        Env env = ctx.getEnv();
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(this.dbName);
        long dbId = db.getId();
        List<List<Comparable>> loadInfos;
        // combine the List<LoadInfo> of load(v1) and loadManager(v2)
        Load load = env.getLoadInstance();
        loadInfos = load.getLoadJobInfosByDbV2(dbId, db.getFullName(), labelValue,
            isAccurateMatch, getLoadStates());
        Set<String> statesValue = getLoadStates() == null ? null : getLoadStates().stream()
                .map(entity -> entity.name())
                .collect(Collectors.toSet());
        if (!Config.isCloudMode()) {
            loadInfos.addAll(env.getLoadManager()
                    .getLoadJobInfosByDb(dbId, labelValue, isAccurateMatch, statesValue));
        } else {
            loadInfos.addAll(((CloudLoadManager) env.getLoadManager())
                    .getLoadJobInfosByDb(dbId, labelValue,
                    isAccurateMatch, statesValue, jobTypes, copyIdValue,
                    isCopyIdAccurateMatch, tableNameValue, isTableNameAccurateMatch,
                    fileValue, isFilesAccurateMatch));
        }
        // add the nerieds load info
        JobManager loadMgr = env.getJobManager();
        loadInfos.addAll(loadMgr.getLoadJobInfosByDb(dbId, db.getFullName(), labelValue,
                isAccurateMatch, getStateV2(), db.getCatalog().getName()));

        // order the result of List<LoadInfo> by orderByPairs in show stmt
        ListComparator<List<Comparable>> comparator;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<>(0);
        }
        Collections.sort(loadInfos, comparator);

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> loadInfo : loadInfos) {
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
        if (offset >= rows.size()) {
            rows = Lists.newArrayList();
        } else if (limit != -1L) {
            if ((limit + offset) < rows.size()) {
                rows = rows.subList((int) offset, (int) (limit + offset));
            } else {
                rows = rows.subList((int) offset, rows.size());
            }
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    private ShowResultSet handleShowStreamLoad() throws AnalysisException {
        Env env = Env.getCurrentEnv();
        Database db = env.getInternalCatalog().getDbOrAnalysisException(this.dbName);
        long dbId = db.getId();

        List<List<Comparable>> streamLoadRecords = env.getStreamLoadRecordMgr()
                .getStreamLoadRecordByDb(dbId, labelValue, isAccurateMatch,
                getStreamState());

        // order the result of List<StreamLoadRecord> by orderByPairst
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(streamLoadRecords, comparator);

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> streamLoadRecord : streamLoadRecords) {
            List<String> oneInfo = new ArrayList<String>(streamLoadRecord.size());

            for (Comparable element : streamLoadRecord) {
                oneInfo.add(element.toString());
            }
            rows.add(oneInfo);
        }

        // filter by limit
        if (offset >= rows.size()) {
            rows = Lists.newArrayList();
        } else if (limit != -1L) {
            if ((limit + offset) < rows.size()) {
                rows = rows.subList((int) offset, (int) (limit + offset));
            } else {
                rows = rows.subList((int) offset, rows.size());
            }
        }
        return new ShowResultSet(getMetaData(), rows);
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
    }

    public String getLabelValue() {
        return labelValue;
    }

    public String getStateValue() {
        return stateValue;
    }

    private Set<JobState> getLoadStates() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }

        Set<JobState> states = new HashSet<JobState>();
        JobState state = JobState.valueOf(stateValue);
        states.add(state);

        if (state == JobState.FINISHED) {
            states.add(JobState.QUORUM_FINISHED);
        }
        return states;
    }

    private StreamLoadState getStreamState() {
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

    public boolean isAccurateMatch() {
        return isAccurateMatch;
    }

    public org.apache.doris.load.loadv2.JobState getStateV2() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }
        return org.apache.doris.load.loadv2.JobState.valueOf(stateValue);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowLoadCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (isStreamLoad) {
            for (String title : SREAMD_LOAD_TITLE_NAMES) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        } else {
            for (String title : LOAD_TITLE_NAMES) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        }
        return builder.build();
    }
}
