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
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.jdbc.source.JdbcScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;

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
        JdbcExternalCatalog catalog = (JdbcExternalCatalog) catalogIf;
        return catalog.getColumnsFromQuery(query);
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc) {
        JdbcExternalCatalog catalog = (JdbcExternalCatalog) catalogIf;
        JdbcTable jdbcTable = new JdbcTable(1, desc.getTable().getName(), desc.getTable().getFullSchema(),
                TableType.JDBC);
        catalog.configureJdbcTable(jdbcTable, desc.getTable().getName());
        desc.setTable(jdbcTable);
        return new JdbcScanNode(id, desc, true, query);
    }
}
