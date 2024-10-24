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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogMgr;

import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * MTMVPartitionInfo
 */
public class MTMVPartitionInfo {

    public enum MTMVPartitionType {
        FOLLOW_BASE_TABLE,
        EXPR,
        SELF_MANAGE
    }

    @SerializedName("pt")
    private MTMVPartitionType partitionType;
    @SerializedName("rt")
    private BaseTableInfo relatedTable;
    @SerializedName("rc")
    private String relatedCol;
    @SerializedName("pc")
    private String partitionCol;
    @SerializedName("expr")
    private Expr expr;

    public MTMVPartitionInfo() {
    }

    public MTMVPartitionInfo(MTMVPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public MTMVPartitionInfo(MTMVPartitionType partitionType,
            String partitionCol) {
        this.partitionType = partitionType;
        this.partitionCol = partitionCol;
    }

    public MTMVPartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(MTMVPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public BaseTableInfo getRelatedTableInfo() {
        return relatedTable;
    }

    public MTMVRelatedTableIf getRelatedTable() throws AnalysisException {
        return (MTMVRelatedTableIf) MTMVUtil.getTable(relatedTable);
    }

    public void setRelatedTable(BaseTableInfo relatedTable) {
        this.relatedTable = relatedTable;
    }

    public String getRelatedCol() {
        return relatedCol;
    }

    public void setRelatedCol(String relatedCol) {
        this.relatedCol = relatedCol;
    }

    public String getPartitionCol() {
        return partitionCol;
    }

    public void setPartitionCol(String partitionCol) {
        this.partitionCol = partitionCol;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }

    /**
     * Get the position of relatedCol in the relatedTable partition column
     *
     * @return
     * @throws AnalysisException
     */
    public int getRelatedColPos() throws AnalysisException {
        if (partitionType == MTMVPartitionType.SELF_MANAGE) {
            throw new AnalysisException("partitionType is: " + partitionType);
        }
        List<Column> partitionColumns = getRelatedTable().getPartitionColumns();
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (partitionColumns.get(i).getName().equalsIgnoreCase(relatedCol)) {
                return i;
            }
        }
        throw new AnalysisException(
                String.format("getRelatedColPos error, relatedCol: %s, partitionColumns: %s", relatedCol,
                        partitionColumns));
    }

    // toString() is not easy to find where to call the method
    public String toInfoString() {
        return "MTMVPartitionInfo{"
                + "partitionType=" + partitionType
                + ", relatedTable=" + relatedTable
                + ", relatedCol='" + relatedCol + '\''
                + ", partitionCol='" + partitionCol + '\''
                + '}';
    }

    public String toNameString() {
        if (partitionType == MTMVPartitionType.SELF_MANAGE) {
            return "MTMVPartitionInfo{"
                    + "partitionType=" + partitionType
                    + '}';
        } else {
            return "MTMVPartitionInfo{"
                    + "partitionType=" + partitionType
                    + ", relatedTable=" + relatedTable.getTableName()
                    + ", relatedCol='" + relatedCol + '\''
                    + ", partitionCol='" + partitionCol + '\''
                    + '}';
        }
    }

    public void compatible(CatalogMgr catalogMgr) {
        if (relatedTable == null) {
            return;
        }
        relatedTable.compatible(catalogMgr);
    }
}
