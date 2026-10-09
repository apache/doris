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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

/**
 * AlterStreamCommand, e.g. ALTER STREAM s1 SET COMMENT 'new comment'
 */
public class AlterStreamCommand extends AlterCommand {
    /**
     * The kind of alteration applied to the stream.
     */
    public enum AlterType {
        SET_COMMENT
    }

    private final TableNameInfo streamName;
    private final AlterType alterType;
    private final String comment;

    public AlterStreamCommand(TableNameInfo streamName, AlterType alterType, String comment) {
        super(PlanType.ALTER_STREAM_COMMAND);
        this.streamName = streamName;
        this.alterType = alterType;
        this.comment = comment;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);

        DatabaseIf db = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrDdlException(streamName.getCtl())
                .getDbOrDdlException(streamName.getDb());
        TableIf table = db.getTableOrDdlException(streamName.getTbl());
        if (!(table instanceof BaseTableStream)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, streamName.getDb(), streamName.getTbl(),
                    "STREAM", "Use 'ALTER TABLE " + streamName.getTbl() + "'");
        }

        switch (alterType) {
            case SET_COMMENT:
                Env.getCurrentEnv().getAlterInstance()
                        .processAlterStreamComment(db.getId(), (BaseTableStream) table, comment);
                break;
            default:
                throw new UserException("Unsupported alter stream operation: " + alterType);
        }
    }

    private void validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(streamName.getDb())) {
            streamName.setDb(ctx.getDatabase());
        }
        streamName.analyze(ctx.getNameSpaceContext());
        InternalDatabaseUtil.checkDatabase(streamName.getDb(), ctx);
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ctx, streamName.getCtl(), streamName.getDb(), streamName.getTbl(),
                        PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER STREAM",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(), streamName.getDb() + ": " + streamName.getTbl());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterStreamCommand(this, context);
    }

    public TableNameInfo getStreamName() {
        return streamName;
    }

    public AlterType getAlterType() {
        return alterType;
    }

    public String getComment() {
        return comment;
    }
}
