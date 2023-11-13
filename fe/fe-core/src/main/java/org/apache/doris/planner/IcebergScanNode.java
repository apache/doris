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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.IcebergProperty;
import org.apache.doris.catalog.IcebergTable;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.catalog.external.IcebergExternalTable;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.external.iceberg.IcebergApiSource;
import org.apache.doris.planner.external.iceberg.IcebergHMSSource;
import org.apache.doris.planner.external.iceberg.IcebergSource;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExplainLevel;

import com.alibaba.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class IcebergScanNode extends BrokerScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergSource source;
    private Table icebergTable;
    private final List<Expression> icebergPredicates = new ArrayList<>();

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                           List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        super(id, desc, planNodeName, fileStatusesList, filesAdded, StatisticalType.ICEBERG_SCAN_NODE);

        ExternalTable table = (ExternalTable) desc.getTable();
        if (table instanceof HMSExternalTable) {
            source = new IcebergHMSSource((HMSExternalTable) table, desc, columnNameToRange);
        } else if (table instanceof IcebergExternalTable) {
            String catalogType = ((IcebergExternalTable) table).getIcebergCatalogType();
            switch (catalogType) {
                case IcebergExternalCatalog.ICEBERG_HMS:
                case IcebergExternalCatalog.ICEBERG_REST:
                    source = new IcebergApiSource((IcebergExternalTable) table, desc, columnNameToRange);
                    break;
                default:
                    Preconditions.checkState(false, "Unknown iceberg catalog type: " + catalogType);
                    break;
            }
        }
        Preconditions.checkNotNull(source);
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        icebergTable = Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache().getIcebergTable(source);
        super.init(analyzer);
    }

    @Override
    protected void initFileGroup() throws UserException {
        IcebergTable table = (IcebergTable) icebergTable;
        fileGroups = Lists.newArrayList(
            new BrokerFileGroup(table.getId(),
                null,
                table.getFileFormat()));
        brokerDesc = new BrokerDesc("IcebergTableDesc", table.getStorageType(),
            table.getIcebergProperties());
        targetTable = table;
    }

    @Override
    public String getHostUri() throws UserException {
        return ((IcebergTable) icebergTable).getHostUri();
    }

    @Override
    protected void getFileStatus() throws UserException {
        throw new UserException("IcebergScanNode is deprecated");
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!isLoad()) {
            output.append(prefix).append("TABLE: ").append(icebergTable.name()).append("\n");
            output.append(prefix).append("PATH: ")
                    .append(icebergTable.properties().get(IcebergProperty.ICEBERG_HIVE_METASTORE_URIS))
                    .append("\n");
        }
        return output.toString();
    }
}

