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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

/*
 * this stmt is now only for convert CLUSTER to TAG SYSTEM
 */
public class AlterClusterStmt extends DdlStmt {
    private Map<String, String> properties;
    private String clusterName;

    public static String COVERT_TO_TAG_SYSTEM = "convert_to_tag_system";

    public AlterClusterStmt(String clusterName, Map<String, String> properties) {
        this.clusterName = clusterName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_AUTHORITY, "NODE");
        }

        if (properties == null || properties.size() == 0) {
            throw new AnalysisException("No property specified");
        }

        if (properties.size() != 1) {
            throw new AnalysisException("Only allow one property at a time");
        }

        if (!properties.containsKey(COVERT_TO_TAG_SYSTEM) || !properties.get(COVERT_TO_TAG_SYSTEM).equals("true")) {
            throw new AnalysisException("Invalid property: " + properties);
        }
    }

    @Override
    public String toSql() {
        return "ALTER CLUSTER " + clusterName + " PROPERTIES(\"convert_to_tag_system\"=\"true\")";
    }

    public String getClusterName() {
        return this.clusterName;
    }
}
