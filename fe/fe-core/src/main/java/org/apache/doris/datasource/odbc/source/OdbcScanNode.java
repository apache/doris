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

package org.apache.doris.datasource.odbc.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;

/**
 * @deprecated ODBC tables are no longer supported. This class is retained only for
 * compilation compatibility. It will throw {@link UnsupportedOperationException}
 * if any attempt is made to use it at runtime.
 */
@Deprecated
public class OdbcScanNode extends ExternalScanNode {

    public OdbcScanNode(PlanNodeId id, TupleDescriptor desc, OdbcTable tbl, ScanContext scanContext) {
        super(id, desc, "SCAN ODBC", scanContext, false);
        throw new UnsupportedOperationException(
                "ODBC tables are no longer supported. Please use JDBC Catalog instead.");
    }

    @Override
    public void init() throws UserException {
        throw new UnsupportedOperationException("ODBC tables are no longer supported.");
    }

    @Override
    public void finalizeForNereids() throws UserException {
        throw new UnsupportedOperationException("ODBC tables are no longer supported.");
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        throw new UnsupportedOperationException("ODBC tables are no longer supported.");
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return prefix + "ODBC tables are deprecated.\n";
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        throw new UnsupportedOperationException("ODBC tables are no longer supported.");
    }

    @Override
    public int getNumInstances() {
        return 1;
    }
}
