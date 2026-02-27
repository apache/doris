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
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * MTMVPartitionInfo
 */
public class MTMVPartitionInfo implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(MTMVPartitionInfo.class);

    public enum MTMVPartitionType {
        FOLLOW_BASE_TABLE,
        EXPR,
        SELF_MANAGE
    }

    @SerializedName("pt")
    private MTMVPartitionType partitionType;
    // old version only support one pct table
    @Deprecated
    @SerializedName("rt")
    private BaseTableInfo relatedTable;
    @Deprecated
    @SerializedName("rc")
    private String relatedCol;
    @SerializedName("pc")
    private String partitionCol;
    @SerializedName("expr")
    private Expr expr;
    @SerializedName("pi")
    private List<BaseColInfo> pctInfos = Lists.newArrayList();

    public MTMVPartitionInfo() {
        this.pctInfos = Lists.newArrayList();
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

    @Deprecated
    public BaseTableInfo getRelatedTableInfo() {
        return relatedTable;
    }

    @Deprecated
    public MTMVRelatedTableIf getRelatedTable() throws AnalysisException {
        return (MTMVRelatedTableIf) MTMVUtil.getTable(relatedTable);
    }

    public Set<MTMVRelatedTableIf> getPctTables() throws AnalysisException {
        Set<MTMVRelatedTableIf> res = Sets.newHashSetWithExpectedSize(pctInfos.size());
        for (BaseColInfo baseColInfo : pctInfos) {
            res.add((MTMVRelatedTableIf) MTMVUtil.getTable(baseColInfo.getTableInfo()));
        }
        return res;
    }

    public List<BaseColInfo> getPctInfos() {
        return pctInfos;
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
     * Get the position of pct col in the pctTable partition column
     *
     * @return
     * @throws AnalysisException
     */
    public int getPctColPos(MTMVRelatedTableIf pctTable) throws AnalysisException {
        if (partitionType == MTMVPartitionType.SELF_MANAGE) {
            throw new AnalysisException("partitionType is: " + partitionType);
        }
        BaseColInfo pctInfo = getPctInfoByPctTable(pctTable);
        List<Column> partitionColumns = pctTable.getPartitionColumns(
                MvccUtil.getSnapshotFromContext(pctTable));
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (partitionColumns.get(i).getName().equalsIgnoreCase(pctInfo.getColName())) {
                return i;
            }
        }
        throw new AnalysisException(
                String.format("getPctColPos error, pctCol: %s, partitionColumns: %s", pctInfo.getColName(),
                        partitionColumns));
    }

    public String getPartitionColByPctTable(MTMVRelatedTableIf pctTable) throws AnalysisException {
        BaseColInfo pctInfoByPctTable = getPctInfoByPctTable(pctTable);
        return pctInfoByPctTable.getColName();
    }

    private BaseColInfo getPctInfoByPctTable(MTMVRelatedTableIf pctTable) throws AnalysisException {
        BaseTableInfo pctInfo = new BaseTableInfo(pctTable);
        for (BaseColInfo baseColInfo : pctInfos) {
            if (baseColInfo.getTableInfo().equals(pctInfo)) {
                return baseColInfo;
            }
        }
        throw new AnalysisException("not have this pct table");
    }

    public void setPctInfos(List<BaseColInfo> pctInfos) {
        this.pctInfos = pctInfos;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MTMVPartitionInfo that = (MTMVPartitionInfo) o;
        return partitionType == that.partitionType && Objects.equals(partitionCol, that.partitionCol)
                && Objects.equals(expr, that.expr) && Objects.equals(pctInfos, that.pctInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionType, partitionCol, expr, pctInfos);
    }

    // toString() is not easy to find where to call the method
    public String toInfoString() {
        return "MTMVPartitionInfo{"
                + "partitionType=" + partitionType
                + ", pctInfos=" + pctInfos
                + ", partitionCol='" + partitionCol + '\''
                + ", expr='" + getExprString() + '\''
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
                    + ", pctInfos=" + pctInfos
                    + ", partitionCol='" + partitionCol + '\''
                    + ", expr='" + getExprString() + '\''
                    + '}';
        }
    }

    private String getExprString() {
        if (expr == null) {
            return null;
        }
        try {
            return MTMVPartitionExprFactory.getExprService(expr).toSql(this);
        } catch (AnalysisException e) {
            // should not happen
            LOG.warn("getExprString", e);
            return null;
        }
    }

    public void compatible(CatalogMgr catalogMgr) throws Exception {
        if (relatedTable == null) {
            return;
        }
        relatedTable.compatible(catalogMgr);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (relatedTable != null && CollectionUtils.isEmpty(pctInfos)) {
            pctInfos.add(new BaseColInfo(relatedCol, relatedTable));
        }
    }
}
