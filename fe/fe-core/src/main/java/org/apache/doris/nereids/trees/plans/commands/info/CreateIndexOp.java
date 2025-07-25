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
import org.apache.doris.analysis.CreateIndexClause;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.catalog.Index;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * CreateIndexOp
 */
public class CreateIndexOp extends AlterTableOp {
    // index definition class
    private IndexDefinition indexDef;
    // when alter = true, clause like: alter table add index xxxx
    // when alter = false, clause like: create index xx on table xxxx
    private boolean alter;
    // index internal class
    private Index index;

    public CreateIndexOp(TableNameInfo tableName, IndexDefinition indexDef, boolean alter) {
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

    public TableNameInfo getTableName() {
        return tableName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (indexDef == null) {
            throw new AnalysisException("index definition expected.");
        }
        if (tableName != null) {
            tableName.analyze(ctx);
        }

        if (indexDef.getIndexType() == IndexDef.IndexType.ANN) {
            throw new AnalysisException(
                "ANN index can only be created during table creation, not through CREATE INDEX.");
        }

        indexDef.validate();
        index = indexDef.translateToCatalogStyle();
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new CreateIndexClause(tableName != null ? tableName.transferToTableName() : null,
                indexDef.translateToLegacyIndexDef(), index, alter);
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
            return "CREATE " + indexDef.toSql(tableName != null ? tableName.toSql() : null);
        }
    }
}
