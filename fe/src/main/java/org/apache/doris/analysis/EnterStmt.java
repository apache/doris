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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import com.google.common.base.Strings;

public class EnterStmt extends DdlStmt {

    private String name;

    public EnterStmt(String name) {
        this.name = name;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (Strings.isNullOrEmpty(name)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NAME_NULL);
        }

        if (analyzer.getCatalog().getCluster(name) == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_UNKNOWN_ERROR, name);
        }
    }

    @Override
    public String toSql() {
        // TODO Auto-generated method stub
        return super.toSql();
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return super.toString();
    }

    public String getClusterName() {
        return name;
    }

    public void setClusterName(String name) {
        this.name = name;
    }
    
    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
    
}
