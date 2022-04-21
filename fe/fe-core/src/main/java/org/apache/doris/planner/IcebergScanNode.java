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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.IcebergProperty;
import org.apache.doris.catalog.IcebergTable;
import org.apache.doris.common.UserException;
import org.apache.doris.external.iceberg.util.IcebergUtils;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExplainLevel;

import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class IcebergScanNode extends BrokerScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergTable icebergTable;
    private final List<Expression> icebergPredicates = new ArrayList<>();

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                           List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        super(id, desc, planNodeName, fileStatusesList, filesAdded, NodeType.ICEBREG_SCAN_NODE);
        icebergTable = (IcebergTable) desc.getTable();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
    }

    @Override
    protected void initFileGroup() throws UserException {
        fileGroups = Lists.newArrayList(new BrokerFileGroup(icebergTable));
        brokerDesc = new BrokerDesc("IcebergTableDesc", icebergTable.getStorageType(),
                icebergTable.getIcebergProperties());
        targetTable = icebergTable;
    }

    @Override
    public String getHostUri() throws UserException {
        return icebergTable.getHostUri();
    }

    @Override
    protected void getFileStatus() throws UserException {
        // extract iceberg conjuncts
        ListIterator<Expr> it = conjuncts.listIterator();
        while (it.hasNext()) {
            Expression expression = IcebergUtils.convertToIcebergExpr(it.next());
            if (expression != null) {
                icebergPredicates.add(expression);
            }
        }
        // get iceberg file status
        List<TBrokerFileStatus> fileStatuses;
        try {
            fileStatuses = icebergTable.getIcebergDataFiles(icebergPredicates);
        } catch (Exception e) {
            LOG.warn("errors while load iceberg table {} data files.", icebergTable.getName(), e);
            throw new UserException("errors while load Iceberg table ["
                    + icebergTable.getName() + "] data files.");
        }
        fileStatusesList.add(fileStatuses);
        filesAdded += fileStatuses.size();
        for (TBrokerFileStatus fstatus : fileStatuses) {
            LOG.debug("Add file status is {}", fstatus);
        }
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!isLoad()) {
            output.append(prefix).append("TABLE: ").append(icebergTable.getName()).append("\n");
            output.append(prefix).append("PATH: ")
                    .append(icebergTable.getIcebergProperties().get(IcebergProperty.ICEBERG_HIVE_METASTORE_URIS))
                    .append("\n");
        }
        return output.toString();
    }
}
