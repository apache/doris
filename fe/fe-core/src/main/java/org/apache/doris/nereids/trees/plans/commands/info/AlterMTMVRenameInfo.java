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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.qe.ConnectContext;

import java.util.Objects;

/**
 * rename
 */
public class AlterMTMVRenameInfo extends AlterMTMVInfo {
    private final String newName;

    /**
     * constructor for alter MTMV
     */
    public AlterMTMVRenameInfo(TableNameInfo mvName, String newName) {
        super(mvName);
        this.newName = Objects.requireNonNull(newName, "require newName object");
    }

    public void analyze(ConnectContext ctx) throws AnalysisException {
        super.analyze(ctx);
        FeNameFormat.checkTableName(newName);
    }

    @Override
    public void run() throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(mvName.getDb());
        Table table = db.getTableOrDdlException(mvName.getTbl());
        Env.getCurrentEnv().renameTable(db, table, newName);
    }
}
