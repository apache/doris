// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.qe.ConnectContext;

import org.apache.commons.lang.NotImplementedException;

import java.util.List;

@Deprecated
public class AlterUserStmt extends DdlStmt {
    private UserIdentity userIdent;
    private AlterUserClause clause;
    
    public AlterUserStmt(UserIdentity userIdent, AlterClause clause) {
        this.userIdent = userIdent;
        this.clause = (AlterUserClause) clause;
    }
    
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        
        userIdent.analyze(analyzer.getClusterName());
        
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER USER");
        }

        // alter clause analysis
        clause.analyze(analyzer);        
    }
    
    public UserIdentity getUserIdent() {
        return userIdent;
    }
    
    public List<String> getHosts() {
        return clause.getHosts();
    }
   
    public List<String> getIps() {
        return clause.getIps();
    }

    public List<String> getStarIps() {
        return clause.getStarIps();
    }
    
    public AlterUserType getAlterUserType() {
        return clause.getAlterUserType();
    }
    
    @Override
    public String toSql() {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        throw new NotImplementedException();
    }


}
