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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;
import java.util.Objects;

/**
 * replace
 */
public class AlterMTMVReplaceInfo extends AlterMTMVInfo {
    protected final String newName;
    private final Map<String, String> properties;

    // parsed from properties.
    // if false, after replace, there will be only one table exist with.
    // if true, the new table and the old table will be exchanged.
    // default is true.
    private boolean swapTable;

    /**
     * constructor for alter MTMV
     */
    public AlterMTMVReplaceInfo(TableNameInfo mvName, String newName, Map<String, String> properties) {
        super(mvName);
        this.newName = Objects.requireNonNull(newName, "require newName object");
        this.properties = Objects.requireNonNull(properties, "require properties object");
    }

    /**
     * analyze
     *
     * @param ctx ctx
     * @throws AnalysisException AnalysisException
     */
    public void analyze(ConnectContext ctx) throws AnalysisException {
        super.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, mvName.getCtl(), mvName.getDb(),
                newName, PrivPredicate.ALTER)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("ALTER",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(),
                    mvName.getDb() + ": " + newName);
            throw new AnalysisException(message);
        }
        this.swapTable = PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_SWAP_TABLE, true);

        if (properties != null && !properties.isEmpty()) {
            throw new AnalysisException("Unknown properties: " + properties.keySet());
        }
        // check new mv exist
        try {
            Env.getCurrentInternalCatalog().getDbOrAnalysisException(mvName.getDb())
                    .getTableOrDdlException(newName,
                            TableType.MATERIALIZED_VIEW);
        } catch (DdlException | org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    public void run() throws UserException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(mvName.getDb());
        MTMV mtmv = (MTMV) db.getTableOrDdlException(mvName.getTbl(), TableType.MATERIALIZED_VIEW);
        MTMV newMtmv = (MTMV) db.getTableOrDdlException(newName, TableType.MATERIALIZED_VIEW);
        Env.getCurrentEnv().getAlterInstance().processReplaceTable(db, mtmv, newName, swapTable);
        Env.getCurrentEnv().getMtmvService().alterTable(newMtmv, mvName.getTbl());
        if (swapTable) {
            Env.getCurrentEnv().getMtmvService().alterTable(mtmv, newName);
        } else {
            Env.getCurrentEnv().getMtmvService().dropMTMV(mtmv);
            Env.getCurrentEnv().getMtmvService().dropTable(mtmv);
        }
    }
}
