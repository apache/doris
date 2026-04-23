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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.PassthroughQueryTableHandle;
import org.apache.doris.datasource.ConnectorColumnConverter;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class JdbcQueryTableValueFunction extends QueryTableValueFunction {
    public static final Logger LOG = LogManager.getLogger(JdbcQueryTableValueFunction.class);

    public JdbcQueryTableValueFunction(Map<String, String> params) throws AnalysisException {
        super(params);
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalogIf;
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = pluginCatalog.getConnector().getMetadata(session);
        ConnectorTableSchema schema = metadata.getColumnsFromQuery(session, query);
        return ConnectorColumnConverter.convertColumns(schema.getColumns());
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalogIf;
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        PassthroughQueryTableHandle queryHandle = new PassthroughQueryTableHandle(query);
        return new PluginDrivenScanNode(id, desc, false, sv,
                ScanContext.builder().clusterName(sv.resolveCloudClusterName()).build(),
                pluginCatalog.getConnector(), session, queryHandle);
    }
}
