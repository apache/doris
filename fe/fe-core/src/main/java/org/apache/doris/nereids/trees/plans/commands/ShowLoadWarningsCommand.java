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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.load.CloudLoadManager;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

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
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Show load warnings command.
 */
public class ShowLoadWarningsCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowLoadWarningsCommand.class);

    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
        .addColumn(new Column("JobId", ScalarType.createVarchar(15)))
        .addColumn(new Column("Label", ScalarType.createVarchar(15)))
        .addColumn(new Column("ErrorMsgDetail", ScalarType.createVarchar(100)))
        .build();

    private String dbName;
    private Expression wildWhere;
    private long limit = 100;
    private URL url;

    public ShowLoadWarningsCommand(String dbName, Expression wildWhere, long limit, URL url) {
        super(PlanType.SHOW_LOAD_WARNINGS_COMMAND);
        this.dbName = dbName;
        this.wildWhere = wildWhere;
        this.limit = limit;
        this.url = url;
    }

    private boolean isFindByLabel() {
        return wildWhere != null && wildWhere instanceof EqualTo && ((EqualTo) wildWhere).left() instanceof UnboundSlot
            && ((UnboundSlot) ((EqualTo) wildWhere).left()).getName().equalsIgnoreCase("label");
    }

    private boolean isFindByJobId() {
        return wildWhere != null && wildWhere instanceof EqualTo && ((EqualTo) wildWhere).left() instanceof UnboundSlot
            && ((UnboundSlot) ((EqualTo) wildWhere).left()).getName().equalsIgnoreCase("load_job_id");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowLoadWarningsCommand(this, context);
    }

    private List<List<String>> showLoadWarningsFromUrl(URL url) throws Exception {
        List<List<String>> rows = Lists.newArrayList();
        // url should like:
        // http://be_ip:be_http_port/api/_load_error_log?file=__shard_xxx/error_log_xxx
        String host = url.getHost();
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        } else {
            host = url.getHost();
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

        URLConnection urlConnection = url.openConnection();
        InputStream inputStream = urlConnection.getInputStream();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            while (reader.ready() && limit > 0) {
                String line = reader.readLine();
                rows.add(Lists.newArrayList("-1", FeConstants.null_string, line));
                limit--;
            }
        }
        return rows;
    }

    private String getLabet(Expression right) {
        return ((VarcharLiteral) right).getValue();
    }

    private long getJobId(Expression right) {
        return ((IntegerLikeLiteral) right).getNumber().longValue();
    }

    private void checkAuth(Database db, String label) throws Exception{
        Load load = Env.getCurrentEnv().getLoadInstance();
        long jobId = 0;
        jobId = load.getLatestJobIdByLabel(db.getId(), label);
        LoadJob job = load.getLoadJob(jobId);
        if (job != null) {
            Set<String> tableNames = job.getTableNames();
            if (tableNames.isEmpty()) {
                // forward compatibility
                if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                        PrivPredicate.SHOW)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                        ConnectContext.get().getQualifiedUser(), db.getFullName());
                }
            } else {
                for (String tblName : tableNames) {
                    if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                            tblName, PrivPredicate.SHOW)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW LOAD WARNINGS",
                            ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                            db.getFullName() + ": " + tblName);
                    }
                }
            }
        }
    }

    private List<List<String>> showLoadWarningsFromLabel(Database db, Expression right) throws Exception {
        String label = getLabet(right);

        //from mysql load manager
        String urlString = Env.getCurrentEnv().getLoadManager().getMysqlLoadManager().getErrorUrlByLoadId(label);
        if (urlString != null && !urlString.isEmpty()) {
            URL url;
            try {
                url = new URL(urlString);
            } catch (MalformedURLException e) {
                throw new AnalysisException("Invalid url: " + e.getMessage());
            }
            return showLoadWarningsFromUrl(url);
        }

        //from load manager
        LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
        List<List<Comparable>> loadJobInfosByDb;
        if (!Config.isCloudMode()) {
            loadJobInfosByDb = loadManager.getLoadJobInfosByDb(db.getId(),
                label,
                true, null);
        } else {
            loadJobInfosByDb = ((CloudLoadManager) loadManager)
                .getLoadJobInfosByDb(db.getId(),
                    label,
                    true, null, null, null, false, null, false, null, false);
        }
        if (CollectionUtils.isEmpty(loadJobInfosByDb)) {
            throw new AnalysisException("job is not exist.");
        }
        List<List<String>> infoList = Lists.newArrayListWithCapacity(loadJobInfosByDb.size());
        for (List<Comparable> comparables : loadJobInfosByDb) {
            List<String> singleInfo = comparables.stream().map(Object::toString).collect(Collectors.toList());
            infoList.add(singleInfo);
        }

        checkAuth(db, label);
        return infoList;
    }

    private List<List<String>> showLoadWarningsFromJobId(Database db, Expression right) throws Exception {
        long jobId = getJobId(right);
        LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
        org.apache.doris.load.loadv2.LoadJob loadJob = loadManager.getLoadJob(jobId);
        if (loadJob == null) {
            throw new AnalysisException("job is not exist.");
        }
        String label = loadJob.getLabel();
        LOG.info("label={}", label);
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("load_job_id={}", jobId);
        }

        checkAuth(db, label);
        return Lists.newArrayList(Collections.singleton(singleInfo));
    }

    private List<List<String>> eqExpression(Expression eqExpr) throws Exception {
        List<List<String>> rows = null;
        if (!(eqExpr instanceof EqualTo)) {
            throw new AnalysisException("show load warnings only support equal condition, but not: " + eqExpr.toSql());
        }

        Expression left = ((EqualTo) eqExpr).left();
        Expression right = ((EqualTo) eqExpr).right();

        if (!isFindByJobId() && !isFindByLabel()) {
            LOG.warn("Current not support left child of where: {}", left);
            throw new AnalysisException("should supply condition like: LABEL = \"your_load_label\","
                + " or LOAD_JOB_ID = $job_id");
        }
        if (isFindByJobId() && !right.getDataType().isNumericType()) {
            LOG.warn("load_job_id is not IntegerType. value: {}", eqExpr.toSql());
            throw new AnalysisException("should supply condition like: LABEL = \"your_load_label\","
                + " or LOAD_JOB_ID = $job_id");
        } else if (isFindByLabel() && !right.getDataType().isVarcharType()) {
            LOG.warn("load_job_id is not StringType. value: {}", eqExpr.toSql());
            throw new AnalysisException("should supply condition like: LABEL = \"your_load_label\","
                + " or LOAD_JOB_ID = $job_id");
        }

        Env env = Env.getCurrentEnv();
        Database db = env.getInternalCatalog().getDbOrAnalysisException(dbName);
        if (isFindByLabel()) {
            rows = showLoadWarningsFromLabel(db, right);
        } else if (isFindByJobId()) {
            rows = showLoadWarningsFromJobId(db, right);
        }


        return rows;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {

        List<List<String>> rows = Lists.newArrayList();

        if (url != null) {
            rows.addAll(showLoadWarningsFromUrl(url));
        } else {
            if (dbName == null || dbName.isEmpty()) {
                dbName = ctx.getDatabase();
                if (dbName == null || dbName.isEmpty()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }
            if (wildWhere instanceof CompoundPredicate) {
                if (!(wildWhere instanceof Or)) {
                    throw new AnalysisException("Only allow compound predicate with operator OR");
                }
                CompoundPredicate cp = (CompoundPredicate) wildWhere;
                rows.addAll(eqExpression(cp.child(0)));
                rows.addAll(eqExpression(cp.child(1)));
            } else {
                rows.addAll(eqExpression(wildWhere));
            }
        }

        if (limit != -1L && limit < rows.size()) {
            rows = rows.subList(0, (int) limit);
        }

        return new ShowResultSet(META_DATA, rows);
    }
}
