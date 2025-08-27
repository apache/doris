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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ProfileManager;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.manager.QueryProfileAction.QueryStatistics;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TQueryStatistics;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Insert executor for dictionary table
 */
public class BlackholeInsertExecutor extends AbstractInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(BlackholeInsertExecutor.class);

    /**
     * constructor
     */
    public BlackholeInsertExecutor(ConnectContext ctx, TableIf table, String labelName, NereidsPlanner planner,
            Optional<InsertCommandContext> insertCtx, boolean emptyInsert) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert);
    }

    @Override
    public void beginTransaction() {
    }

    @Override
    protected void beforeExec() throws UserException {
        // do nothing
    }

    @Override
    protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        // do nothing
    }

    @Override
    protected void onComplete() throws UserException {
        if (!coordinator.getExecStatus().ok()) {
            String errMsg = coordinator.getExecStatus().getErrorMsg();
            LOG.warn("blackhole insert failed. label: {}, error: {}", labelName, errMsg);
            throw new UserException(errMsg);
        }
    }

    @Override
    protected void onFail(Throwable t) {
        // must from AbstractInsertExecutor so got DdlException
        DdlException ddlException = (DdlException) t;
        errMsg = t.getMessage() == null ? "unknown reason" : ddlException.getMessage();
        String queryId = DebugUtil.printId(ctx.queryId());
        LOG.warn("blackhole insert [{}] with query id {} failed", labelName, queryId, ddlException);
        StringBuilder sb = new StringBuilder(errMsg);
        if (!Strings.isNullOrEmpty(coordinator.getTrackingUrl())) {
            sb.append(". url: ").append(coordinator.getTrackingUrl());
        }
        // we should set the context to make the caller know the command failed
        ctx.getState().setError(ddlException.getMysqlErrorCode(), sb.toString());
    }

    @Override
    protected void afterExec(StmtExecutor executor) {

    }
}
