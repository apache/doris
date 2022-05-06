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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TSchemaScanNode;
import org.apache.doris.thrift.TUserIdentity;

import com.google.common.base.MoreObjects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Full scan of an SCHEMA table.
 */
public class SchemaScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(SchemaTable.class);

    private final String tableName;
    private String schemaDb;
    private String schemaTable;
    private String schemaWild;
    private String user;
    private String userIp;
    private String frontendIP;
    private int frontendPort;

    /**
     * Constructs node to scan given data files of table 'tbl'.
     */
    public SchemaScanNode(PlanNodeId id, TupleDescriptor desc) {
        super(id, desc, "SCAN SCHEMA", NodeType.SCHEMA_SCAN_NODE);
        this.tableName = desc.getTable().getName();
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        // Convert predicates to MySQL columns and filters.
        schemaDb = analyzer.getSchemaDb();
        schemaTable = analyzer.getSchemaTable();
        schemaWild = analyzer.getSchemaWild();
        user = analyzer.getQualifiedUser();
        userIp = analyzer.getContext().getRemoteIP();
        frontendIP = FrontendOptions.getLocalHostAddress();
        frontendPort = Config.rpc_port;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.SCHEMA_SCAN_NODE;
        msg.schema_scan_node = new TSchemaScanNode(desc.getId().asInt(), tableName);
        if (schemaDb != null) {
            msg.schema_scan_node.setDb(schemaDb);
        } else {
            if (tableName.equalsIgnoreCase("GLOBAL_VARIABLES")) {
                msg.schema_scan_node.setDb("GLOBAL");
            } else if (tableName.equalsIgnoreCase("SESSION_VARIABLES")) {
                msg.schema_scan_node.setDb("SESSION");
            }
        }
        msg.schema_scan_node.show_hidden_cloumns = Util.showHiddenColumns();

        if (schemaTable != null) {
            msg.schema_scan_node.setTable(schemaTable);
        }
        if (schemaWild != null) {
            msg.schema_scan_node.setWild(schemaWild);
        }

        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            msg.schema_scan_node.setThreadId(ConnectContext.get().getConnectionId());
        }
        msg.schema_scan_node.setIp(frontendIP);
        msg.schema_scan_node.setPort(frontendPort);

        TUserIdentity tCurrentUser = ConnectContext.get().getCurrentUserIdentity().toThrift();
        msg.schema_scan_node.setCurrentUserIdent(tCurrentUser);
    }

    /**
     * We query MySQL Meta to get request's data location
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return null;
    }

    @Override
    public int getNumInstances() {
        return 1;
    }
}
