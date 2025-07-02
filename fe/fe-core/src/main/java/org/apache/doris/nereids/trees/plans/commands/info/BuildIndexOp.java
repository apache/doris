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
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * BuildIndexOp
 */
public class BuildIndexOp extends AlterTableOp {
    // index definition class
    private IndexDefinition indexDef;
    // when alter = true, clause like: alter table add index xxxx
    // when alter = false, clause like: create index xx on table xxxx
    private final boolean alter;
    // index internal class
    private Index index;
    // index name
    private final String indexName;
    // partition names info
    private final PartitionNamesInfo partitionNamesInfo;

    public BuildIndexOp(TableNameInfo tableName, String indexName, PartitionNamesInfo partitionNamesInfo,
                            boolean alter) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.tableName = tableName;
        this.indexName = indexName;
        this.partitionNamesInfo = partitionNamesInfo;
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
        tableName.analyze(ctx);
        DatabaseIf<Table> db = Env.getCurrentEnv().getCatalogMgr().getInternalCatalog()
                .getDb(tableName.getDb()).orElse(null);
        if (db == null) {
            throw new AnalysisException("Database[" + tableName.getDb() + "] is not exist");
        }

        TableIf table = db.getTable(tableName.getTbl()).orElse(null);
        if (table == null) {
            throw new AnalysisException("Table[" + tableName.getTbl() + "] is not exist");
        }
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("Only olap table support build index");
        }

        Index existedIdx = null;
        for (Index index : table.getTableIndexes().getIndexes()) {
            if (index.getIndexName().equalsIgnoreCase(indexName)) {
                existedIdx = index;
                if (!existedIdx.isLightIndexChangeSupported()) {
                    throw new AnalysisException("BUILD INDEX operation failed: The index "
                        + existedIdx.getIndexName() + " of type " + existedIdx.getIndexType()
                        + " does not support lightweight index changes.");
                }
                break;
            }
        }
        if (existedIdx == null) {
            throw new AnalysisException("Index[" + indexName + "] is not exist in table[" + tableName.getTbl() + "]");
        }

        IndexDef.IndexType indexType = existedIdx.getIndexType();
        if (indexType == IndexDef.IndexType.NGRAM_BF
                || indexType == IndexDef.IndexType.BLOOMFILTER) {
            throw new AnalysisException(indexType + " index is not needed to build.");
        }

        indexDef = new IndexDefinition(indexName, partitionNamesInfo, indexType);
        if (!table.isPartitionedTable()) {
            List<String> specifiedPartitions = indexDef.getPartitionNames();
            if (!specifiedPartitions.isEmpty()) {
                throw new AnalysisException("table " + table.getName()
                    + " is not partitioned, cannot build index with partitions.");
            }
        }
        indexDef.validate();
        this.index = existedIdx.clone();
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        indexDef.getIndexType();
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
