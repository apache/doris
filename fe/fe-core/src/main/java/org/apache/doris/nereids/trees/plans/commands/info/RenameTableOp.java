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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.TableRenameClause;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Map;

/**
 * TableRenameOp
 */
public class RenameTableOp extends AlterTableOp {
    private String newTableName;

    public RenameTableOp(String newTableName) {
        super(AlterOpType.RENAME);
        this.newTableName = newTableName;
    }

    public String getNewTableName() {
        return newTableName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(newTableName)) {
            throw new AnalysisException("New Table name is not set");
        }

        FeNameFormat.checkTableName(newTableName);
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new TableRenameClause(newTableName);
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return true;
    }

    @Override
    public String toSql() {
        return "RENAME " + newTableName;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
