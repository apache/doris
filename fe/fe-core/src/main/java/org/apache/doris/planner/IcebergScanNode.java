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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.StorageBackend;
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

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class IcebergScanNode extends BrokerScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergTable icebergTable;
    private final List<Expression> icebergPredicates = new ArrayList<>();
    private String hdfsUri;

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                           List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        super(id, desc, planNodeName, fileStatusesList, filesAdded);
        icebergTable = (IcebergTable) desc.getTable();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
    }

    @Override
    protected void initFileGroup() throws UserException {
        fileGroups = Lists.newArrayList(new BrokerFileGroup(icebergTable));
        brokerDesc = new BrokerDesc("IcebergTableDesc", StorageBackend.StorageType.HDFS, icebergTable.getIcebergProperties());
        targetTable = icebergTable;
    }

    @Override
    public String getHdfsUri() throws UserException {
        if (Strings.isNullOrEmpty(hdfsUri)) {
            String location = icebergTable.getLocation();
            String[] strings = StringUtils.split(location, "/");
            String[] strs = StringUtils.split(strings[1], ":");
            this.hdfsUri = "hdfs://" + strs[0] + ":" + strs[1];
        }
        return hdfsUri;
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
            LOG.info("Add file status is {}", fstatus);
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
