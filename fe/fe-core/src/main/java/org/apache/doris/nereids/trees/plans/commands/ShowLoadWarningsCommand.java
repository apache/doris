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

import org.apache.doris.analysis.LimitElement;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.load.CloudLoadManager;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * show load warnings command
 */
public class ShowLoadWarningsCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowLoadWarningsCommand.class);
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", ScalarType.createVarchar(128)))
                .addColumn(new Column("Label", ScalarType.createVarchar(128)))
                .addColumn(new Column("ErrorMsgDetail", ScalarType.createVarchar(200)))
                .build();
    private static final String LABEL = "label";
    private static final String LOAD_JOB_ID = "load_job_id";
    private static final String JOB_ID = "JobId";
    private String dbName;
    private URL url;
    private String label;
    private long jobId;
    private final String rawUrl;
    private final Expression wildWhere;
    private final long limit;
    private final long offset;

    /**
     * constructor for show load warnings
     */
    public ShowLoadWarningsCommand(String dbName, Expression wildWhere, long limit, long offset, String rawUrl) {
        super(PlanType.SHOW_LOAD_WARNINGS_COMMAND);
        this.dbName = dbName;
        this.wildWhere = wildWhere;
        this.limit = limit;
        this.offset = offset;
        this.rawUrl = rawUrl;
    }

    private void analyzeUrl() throws AnalysisException {
        try {
            url = new URL(rawUrl);
        } catch (MalformedURLException e) {
            throw new AnalysisException("Invalid url: " + e.getMessage());
        }
    }

    private boolean analyzeSubPredicate(Expression subExpr) throws AnalysisException {
        boolean hasLabel = false;
        boolean hasLoadJobId = false;
        if (subExpr == null) {
            return false;
        }

        if (subExpr instanceof ComparisonPredicate) {
            if (!(subExpr instanceof EqualTo)) {
                return false;
            }
        } else {
            return false;
        }

        // left child
        if (!(subExpr.child(0) instanceof UnboundSlot)) {
            return false;
        }
        String leftKey = ((UnboundSlot) subExpr.child(0)).getName();
        if (leftKey.equalsIgnoreCase(LABEL)) {
            hasLabel = true;
        } else if (leftKey.equalsIgnoreCase(LOAD_JOB_ID) || leftKey.equalsIgnoreCase(JOB_ID)) {
            hasLoadJobId = true;
        } else {
            return false;
        }

        if (hasLabel) {
            if (!(subExpr.child(1) instanceof StringLikeLiteral)) {
                return false;
            }

            String value = ((StringLikeLiteral) subExpr.child(1)).getStringValue();
            if (Strings.isNullOrEmpty(value)) {
                return false;
            }

            label = value;
        }

        if (hasLoadJobId) {
            if (!(subExpr.child(1) instanceof IntegerLikeLiteral)) {
                LOG.warn("load_job_id/jobid is not IntLiteral. value: {}", subExpr.toSql());
                return false;
            }
            jobId = ((IntegerLikeLiteral) subExpr.child(1)).getLongValue();
        }

        return true;
    }

    protected void validate(ConnectContext ctx) throws AnalysisException {
        if (rawUrl != null) {
            // get load error from url
            if (rawUrl.isEmpty()) {
                throw new AnalysisException("Error load url is missing");
            }

            LimitElement limitElement = null;
            if (limit > 0) {
                limitElement = new LimitElement(offset == -1L ? 0 : offset, limit);
            }
            if (dbName != null || wildWhere != null || limitElement != null) {
                throw new AnalysisException(
                        "Can not set database, where or limit clause if getting error log from url");
            }

            // url should like:
            // http://be_ip:be_http_port/api/_load_error_log?file=__shard_xxx/error_log_xxx
            analyzeUrl();
        } else {
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = ctx.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }

            // analyze where clause if not null
            if (wildWhere == null) {
                throw new AnalysisException("should supply condition like: LABEL = \"your_load_label\","
                    + " or LOAD_JOB_ID/JOBID = $job_id");
            }
            boolean valid = true;
            if (wildWhere instanceof CompoundPredicate) {
                if (!(wildWhere instanceof And)) {
                    throw new AnalysisException("Only allow compound predicate with operator AND");
                }
                for (Expression child : wildWhere.children()) {
                    valid &= analyzeSubPredicate(child);
                }
            } else {
                valid = analyzeSubPredicate(wildWhere);
            }
            if (!valid) {
                throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\","
                    + " or LOAD_JOB_ID/JOBID = $job_id");
            }
        }
    }

    private ShowResultSet handleShowLoadWarningsFromURL(URL url) throws AnalysisException {
        String host = url.getHost();
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        int port = url.getPort();
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        Backend be = infoService.getBackendWithHttpPort(host, port);
        if (be == null) {
            throw new AnalysisException(NetUtils.getHostPortInAccessibleFormat(host, port) + " is not a valid backend");
        }
        if (!be.isAlive()) {
            throw new AnalysisException(
                "Backend " + NetUtils.getHostPortInAccessibleFormat(host, port) + " is not alive");
        }

        if (!url.getPath().equals("/api/_load_error_log")) {
            throw new AnalysisException(
                "Invalid error log path: " + url.getPath() + ". path should be: /api/_load_error_log");
        }

        List<List<String>> rows = Lists.newArrayList();
        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.setRequestProperty("Auth-Token", Env.getCurrentEnv().getTokenManager().acquireToken());
            InputStream inputStream = urlConnection.getInputStream();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                int limit = 100;
                while (reader.ready() && limit > 0) {
                    String line = reader.readLine();
                    rows.add(Lists.newArrayList("-1", FeConstants.null_string, line));
                    limit--;
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to get error log from url: " + url, e);
            throw new AnalysisException(
                "failed to get error log from url: " + url + ". reason: " + e.getMessage());
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    private ShowResultSet handleShowLoadWarningV2(Database db) throws AnalysisException {
        LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
        if (label != null) {
            List<List<Comparable>> loadJobInfosByDb;
            if (!Config.isCloudMode()) {
                loadJobInfosByDb = loadManager.getLoadJobInfosByDb(db.getId(), label, true, null);
            } else {
                loadJobInfosByDb = ((CloudLoadManager) loadManager)
                    .getLoadJobInfosByDb(db.getId(), label, true, null, null, null, false, null, false, null, false);
            }
            if (CollectionUtils.isEmpty(loadJobInfosByDb)) {
                throw new AnalysisException("job does not exist");
            }
            List<List<String>> infoList = Lists.newArrayListWithCapacity(loadJobInfosByDb.size());
            for (List<Comparable> comparables : loadJobInfosByDb) {
                List<String> singleInfo = comparables.stream().map(Object::toString).collect(Collectors.toList());
                infoList.add(singleInfo);
            }
            return new ShowResultSet(getMetaData(), infoList);
        }
        org.apache.doris.load.loadv2.LoadJob loadJob = loadManager.getLoadJob(jobId);
        if (loadJob == null) {
            throw new AnalysisException("job does not exist");
        }
        List<String> singleInfo;
        try {
            singleInfo = loadJob
                .getShowInfo()
                .stream()
                .map(Objects::toString)
                .collect(Collectors.toList());
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        return new ShowResultSet(getMetaData(), Lists.newArrayList(Collections.singleton(singleInfo)));
    }

    @VisibleForTesting
    protected ShowResultSet handleShowLoadWarnings(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);

        if (url != null) {
            return handleShowLoadWarningsFromURL(url);
        }

        Env env = Env.getCurrentEnv();
        // try to fetch load id from mysql load first and mysql load only support find by label.
        if (label != null) {
            String urlString = env.getLoadManager().getMysqlLoadManager().getErrorUrlByLoadId(label);
            if (urlString != null && !urlString.isEmpty()) {
                URL url;
                try {
                    url = new URL(urlString);
                } catch (MalformedURLException e) {
                    throw new AnalysisException("Invalid url: " + e.getMessage());
                }
                return handleShowLoadWarningsFromURL(url);
            }
        }

        Database db = env.getInternalCatalog().getDbOrAnalysisException(dbName);
        return handleShowLoadWarningV2(db);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowLoadWarnings(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowLoadWarningsCommand(this, context);
    }
}
