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
import org.apache.doris.analysis.BuildIndexClause;
import org.apache.doris.catalog.Index;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * BuildIndexOp
 */
public class BuildIndexOp extends AlterTableOp {
    // in which table the index on, only used when alter = false
    private final TableNameInfo tableName;
    // index definition class
    private final IndexDefinition indexDef;
    // when alter = true, clause like: alter table add index xxxx
    // when alter = false, clause like: create index xx on table xxxx
    private final boolean alter;
    // index internal class
    private Index index;

    public BuildIndexOp(TableNameInfo tableName, IndexDefinition indexDef, boolean alter) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.tableName = tableName;
        this.indexDef = indexDef;
        this.alter = alter;
    }

    @Override
    public Map<String, String> getProperties() {
        return Maps.newHashMap();
    }

    public Index getIndex() {
        return index;
    }

    public IndexDefinition getIndexDef() {
        return indexDef;
    }

    public boolean isAlter() {
        return alter;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (indexDef == null) {
            throw new AnalysisException("index definition expected.");
        }
        indexDef.validate();
        tableName.analyze(ctx);
        index = indexDef.translateToCatalogStyle();
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new BuildIndexClause(tableName.transferToTableName(), indexDef.translateToLegacyIndexDef(), index,
                alter);
    }

    @Override
    public boolean allowOpMTMV() {
        return true;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        if (alter) {
            return indexDef.toSql();
        } else {
            return "BUILD " + indexDef.toSql(tableName.toSql());
        }
    }
}
