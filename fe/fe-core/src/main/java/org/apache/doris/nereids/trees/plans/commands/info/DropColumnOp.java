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
import org.apache.doris.analysis.DropColumnClause;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Map;
import java.util.Set;

/**
 * DropColumnOp
 */
public class DropColumnOp extends AlterTableOp {
    private String colName;
    private String rollupName;

    private Map<String, String> properties;

    /**
     * DropColumnOp
     */
    public DropColumnOp(String colName, String rollupName, Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.colName = colName;
        this.rollupName = rollupName;
        this.properties = properties;
    }

    public String getColName() {
        return colName;
    }

    public String getRollupName() {
        return rollupName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(colName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                    colName, FeNameFormat.getColumnNameRegex());
        }
        if (colName.startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
            throw new AnalysisException("Do not support drop hidden column");
        }

        TableIf table = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrDdlException(tableName.getCtl())
                .getDbOrDdlException(tableName.getDb())
                .getTableOrDdlException(tableName.getTbl());
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            if (!Strings.isNullOrEmpty(rollupName)) {
                Long indexId = olapTable.getIndexIdByName(rollupName);
                if (indexId != null) {
                    MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
                    if (indexMeta.getDefineStmt() == null) {
                        Column column = indexMeta.getColumnByName(colName);
                        if (column != null) {
                            if (column.isKey()) {
                                if (indexMeta.getSchema(false).stream()
                                        .anyMatch(col -> col.getAggregationType() != null
                                                && col.getAggregationType().isReplaceFamily())) {
                                    throw new AnalysisException(String.format(
                                            "Can not drop key column %s when rollup has value column "
                                                    + "with REPLACE aggregation method",
                                            colName));
                                }
                            }
                        } else {
                            // throw new AnalysisException(String.format("Column[%s] does not exist", colName));
                        }
                    } else {
                        throw new AnalysisException("Can not modify column in mv " + rollupName);
                    }
                } else {
                    // throw new AnalysisException(String.format("rollup[%s] does not exist", rollupName));
                }
            } else {
                Column column = olapTable.getColumn(colName);
                // if (column == null) {
                //     throw new AnalysisException(String.format("Column[%s] does not exist", colName));
                // }

                if (olapTable.getPartitionColumnNames().contains(colName.toLowerCase())
                        || olapTable.getDistributionColumnNames().contains(colName.toLowerCase())) {
                    throw new AnalysisException("Can not drop partition or distribution column : " + colName);
                }
                if (column != null && column.isKey()) {
                    KeysType keysType = olapTable.getKeysType();
                    if (keysType == KeysType.UNIQUE_KEYS) {
                        throw new AnalysisException("Can not drop key column " + colName
                                + " in unique data model table");
                    } else if (keysType == KeysType.AGG_KEYS) {
                        if (olapTable.getFullSchema().stream().anyMatch(
                                col -> col.getAggregationType() != null
                                        && col.getAggregationType().isReplaceFamily())) {
                            throw new AnalysisException("Can not drop key column " + colName
                                    + " in aggregation key table having REPLACE or REPLACE_IF_NOT_NULL column");
                        }
                    }
                }
                long baseIndexId = olapTable.getBaseIndexId();
                for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getVisibleIndexIdToMeta().entrySet()) {
                    long indexId = entry.getKey();
                    if (indexId != baseIndexId) {
                        MaterializedIndexMeta meta = entry.getValue();
                        Set<String> mvUsedColumns = RelationUtil.getMvUsedColumnNames(meta);
                        if (mvUsedColumns.contains(colName)) {
                            throw new AnalysisException(
                                    "Can not drop column contained by mv, mv=" + olapTable.getIndexNameById(indexId));
                        }
                    }
                }
            }
        }
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new DropColumnClause(colName, rollupName, properties);
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
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
        StringBuilder sb = new StringBuilder();
        sb.append("DROP COLUMN `").append(colName).append("`");
        if (rollupName != null) {
            sb.append(" FROM `").append(rollupName).append("`");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
