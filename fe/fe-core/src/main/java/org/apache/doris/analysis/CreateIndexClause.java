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

package org.apache.doris.analysis;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;

import com.google.common.collect.Maps;

import java.util.Map;

public class CreateIndexClause extends AlterTableClause {
    // in which table the index on, only used when alter = false
    private TableNameInfo tableNameInfo;
    // index definition class
    private IndexDefinition indexDef;
    // when alter = true, clause like: alter table add index xxxx
    // when alter = false, clause like: create index xx on table xxxx
    private boolean alter;
    // index internal class
    private Index index;

    public CreateIndexClause(TableNameInfo tableNameInfo, IndexDefinition indexDef, boolean alter) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.tableNameInfo = tableNameInfo;
        this.indexDef = indexDef;
        this.alter = alter;
    }

    // for nereids
    public CreateIndexClause(TableNameInfo tableNameInfo, IndexDefinition indexDef, Index index, boolean alter) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.tableNameInfo = tableNameInfo;
        this.indexDef = indexDef;
        this.index = index;
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
        return tableNameInfo;
    }

    @Override
    public void analyze() throws AnalysisException {
        if (indexDef == null) {
            throw new AnalysisException("index definition expected.");
        }
        indexDef.validate();
        this.index = new Index(Env.getCurrentEnv().getNextId(), indexDef.getIndexName(),
                indexDef.getColumnNames(), indexDef.getIndexType(),
                indexDef.getProperties(), indexDef.getComment());
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
        return toSql(alter);
    }

    public String toSql(boolean alter) {
        if (alter) {
            return "ADD " + indexDef.toSql();
        } else {
            return "CREATE " + indexDef.toSql(tableNameInfo.toSql());
        }
    }
}
