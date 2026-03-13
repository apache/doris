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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

/**
 * stream info in creating table stream.
 */
public class CreateStreamInfo {
    private final TableNameInfo tableStreamName;
    private final TableNameInfo baseTableName;
    private Map<String, String> properties;
    private final boolean ifNotExists;
    private final boolean orReplace;
    private final String comment;

    /**
     * constructor.
     */
    public CreateStreamInfo(boolean ifNotExists, boolean orReplace, TableNameInfo tableStreamName,
                            TableNameInfo baseTableName, Map<String, String> properties, String comment) {
        this.tableStreamName = tableStreamName;
        this.baseTableName = baseTableName;
        this.properties = properties;
        this.ifNotExists = ifNotExists;
        this.orReplace = orReplace;
        this.comment = comment;
    }

    /**
     * validate create table stream info.
     */
    public void validate(ConnectContext ctx) throws UserException {
        tableStreamName.analyze(ctx);
        FeNameFormat.checkTableName(tableStreamName.getTbl());

        // disallow external catalog
        Util.prohibitExternalCatalog(tableStreamName.getCtl(), "CreateStreamCommand");
        // check privilege
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx,
                new TableNameInfo(tableStreamName.getCtl(), tableStreamName.getDb(), tableStreamName.getTbl()),
                PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.CREATE.getPrivs().toString(), tableStreamName.getTbl());
        }

        // check base table
        baseTableName.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx,
                baseTableName.getCtl(), baseTableName.getDb(), baseTableName.getTbl(),
                PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.SELECT.getPrivs().toString(), baseTableName.getTbl());
        }

        InternalDatabaseUtil.checkDatabase(tableStreamName.getDb(), ConnectContext.get());
    }

    public TableNameInfo getStreamName() {
        return this.tableStreamName;
    }

    public TableNameInfo getBaseTableName() {
        return this.baseTableName;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public String getComment() {
        return this.comment;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isOrReplace() {
        return orReplace;
    }

}
