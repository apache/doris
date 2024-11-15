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
import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AddPartitionOp
 */
public class AddPartitionOp extends AlterTableOp {
    private PartitionDefinition partitionDesc;
    private DistributionDescriptor distributionDesc;
    private Map<String, String> properties;
    // true if this is to add a temporary partition
    private boolean isTempPartition;

    /**
     * AddPartitionOp
     */
    public AddPartitionOp(PartitionDefinition partitionDesc,
            DistributionDescriptor distributionDesc,
            Map<String, String> properties,
            boolean isTempPartition) {
        super(AlterOpType.ADD_PARTITION);
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        this.isTempPartition = isTempPartition;

        this.needTableStable = false;
    }

    private SinglePartitionDesc getSingeRangePartitionDesc() {
        SinglePartitionDesc singlePartitionDesc = (SinglePartitionDesc) partitionDesc.translateToCatalogStyle();
        // TODO fe/fe-core/src/main/java/org/apache/doris/datasource/InternalCatalog.java#addPartition
        // will call singlePartitionDesc.analyze method, so have to set analyzed to false to let it work
        singlePartitionDesc.setAnalyzed(false);
        return singlePartitionDesc;
    }

    private DistributionDesc getDistributionDesc() {
        return distributionDesc != null ? distributionDesc.translateToCatalogStyle() : null;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (partitionDesc instanceof StepPartition) {
            throw new AnalysisException("StepPartition is not supported");
        }
        String ctlName = tableName.getCtl();
        String dbName = tableName.getDb();
        String tbName = tableName.getTbl();
        DatabaseIf dbIf = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(ctlName, catalog -> new DdlException("Unknown catalog " + catalog))
                .getDbOrDdlException(dbName);
        TableIf tableIf = dbIf.getTableOrDdlException(tbName);
        if (tableIf instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) tableIf;
            List<DataType> partitionTypes = new ArrayList<>(olapTable.getPartitionColumns().size());
            for (Column col : olapTable.getPartitionColumns()) {
                partitionTypes.add(DataType.fromCatalogType(col.getType()));
            }
            partitionDesc.setPartitionTypes(partitionTypes);
        }
        partitionDesc.validate(properties);
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new AddPartitionClause(getSingeRangePartitionDesc(), getDistributionDesc(), properties, isTempPartition);
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD ");
        sb.append(getSingeRangePartitionDesc().toSql() + "\n");
        if (distributionDesc != null) {
            sb.append(getDistributionDesc().toSql());
        }
        return sb.toString();
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
